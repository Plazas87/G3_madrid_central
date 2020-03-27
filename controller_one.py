#! usr/bin/env python3

import multiprocessing
import concurrent.futures
import time
import pandas as pd
from climate_file_reader import ClimateFileReader
from climate_station_reader import ClimateStation
from traffic_file_reader import TrafficFileReader
from traffic_station_reader import TrafficStation
from db_controller import DatabaseController
import logging
import os
import sys

class Controller:
    """Esta clase es la encargada de tener el comportamiento de toda la aaplicación en orden haciendo uso de
    todas las demas clases que conforman el proyecto. Tieme acceso a todas incluyendo el modelo."""
    def __init__(self, configuration):
        logging.info('Starting controller')
        self.status = False
        self.dbController = DatabaseController(configuration)
        self.airQualityDataController = ClimateFileReader(rutaArchivo=configuration.air_quality_path,
                                                          extencion=['.txt', '.csv'])

        self.airQualityStation = ClimateStation(rutaArchivo=configuration.air_stations_path,
                                                extencion=['.csv'])

        self.trafficDataController = TrafficFileReader(rutaArchivo=configuration.traffic_data_path,
                                                       extencion=['.csv'])

        self.trafficStation = TrafficStation(rutaArchivo=configuration.traffic_station_path,
                                             extencion=['.csv'])

    def start(self):
        self.status = True
        self.read_data()

    def stop(self):
        self.status = False

    def __check_files(self):
        """Esta función se encarga de consultar qué archivos han sido previamente cargados en la base de datos."""
        logging.info('Look for files that has been already loaded')
        query = self.dbController.selectQuery('files', 'file_name', info='Checking for files in files table')
        logging.info('Previously read: ' + str(query))

        return query

    def __check_files_by_path(self, query, obj_files, path):
        logging.info('Checkin files in: {}'.format(path))
        files_read = []
        try:
            # Comprueba que archivos an sido leidos previamente, y por lo tanto no se deben volver a leer
            for i in obj_files:
                if i in query:
                    logging.info('File: ' + str(i) + ' has been already read')
                    pass
                else:
                    logging.info('New file detected: ' + str(i))
                    files_read.append(i)

        except Exception as e:
            logging.error("Can't read files from database " + str(e))

        return files_read

    def read_data(self):
        """Esta función se encarga de leer los archivos que se encuentran en cada una de las ubicaciones del repositorio,
        si el archivo ya ha sido leido, no se carga nuevaente, de lo contrario el archivo se carga normalmente."""
        logging.info('Looking for new files to read')
        load_files = self.__check_files()

        # search for station file inside directory.
        try:
            self.airQualityStation.search_for_files()
            air_station_files_to_read = self.__check_files_by_path(load_files,
                                                                   self.airQualityStation.files,
                                                                   self.airQualityStation.pathName)

            for file in air_station_files_to_read:
                if self.airQualityStation.load_files(files_to_read=file):
                    if self.dbController.insert('station', self.airQualityStation.mainTable, 'Air stations to data base'):
                        self.dbController.insert('files', [file])

        except Exception as e:
            logging.error('Nothing new to read in: {0}-{1}'.format(self.airQualityStation.pathName, e))

        # read files from the air quality path
        try:
            self.airQualityDataController.search_for_files()
            air_quality_files_to_read = self.__check_files_by_path(load_files,
                                                                   self.airQualityDataController.files,
                                                                   self.airQualityDataController.pathName)

            # Bloque de procesos en paralelo para la carga de todos los archivos en la carpeta de calidad del aire.
            start = time.perf_counter()
            processes = []
            ctx = multiprocessing.get_context('spawn')
            queue = ctx.Queue()
            for file in air_quality_files_to_read:
                try:
                    logging.info(f'Start creating a independent for process to read file: {file}')
                    p = ctx.Process(target=self.airQualityDataController.load_files,
                                    args=[queue, [file]])
                    p.start()
                    processes.append(p)
                except Exception as e:
                    logging.error(f"Can't iniciate a process to read file: {file} - {str(e)}")
                    continue

            results = []
            # For para obtener los resultados de la COLA compartida
            for _ in processes:
                results.append(queue.get()[0])

            finish = time.perf_counter()
            logging.info(f'Duración de la carga: {finish - start}')
            # Fin del bloque de procesamiento en paralelo

            self.airQualityDataController.mainTable = pd.concat(results)
            to_db_status, error_data = self.dbController.insert('measurement', self.airQualityDataController.mainTable)

            if to_db_status:
                for file in air_quality_files_to_read:
                    self.dbController.insert('files', [file])

            else:
                self.airQualityDataController.errorFiles.append(error_data)

        except multiprocessing.ProcessError as e:
            logging.error('Parallel process error: {}'.format(e))

        except Exception as e:
            logging.info('Nothing new to read in: {0}-{1}'.format(self.airQualityDataController.pathName, e))

        # read traffic files from directory
        try:
            self.trafficDataController.search_for_files()
            air_station_files_to_read = self.__check_files_by_path(load_files,
                                                                   self.trafficDataController.files,
                                                                   self.trafficDataController.pathName)

            for file in air_station_files_to_read:
                logging.info(f'File to Database: {file}')
                if self.dbController.insert('traffic2', []):
                    logging.info(f'Precess complete with: {file}')
                    self.dbController.insert('files', [file])
                else:
                    logging.error(f'Can not process the file: {file}')

        except Exception as e:
            logging.error('Nothing new to read in: {0}-{1}'.format(self.trafficDataController.pathName, e))

        # read traffic station files from directory
        try:
            self.trafficStation.search_for_files()
            traffic_station_files_to_read = self.__check_files_by_path(load_files,
                                                                   self.trafficStation.files,
                                                                   self.trafficStation.pathName)
            if len(traffic_station_files_to_read) != 0:
                logging.info(f'File to Database: {traffic_station_files_to_read}')
                self.trafficStation.load_files(files_to_read=traffic_station_files_to_read)
                self.trafficStation.prepare_data()
                if self.dbController.insert('trafficstation', self.trafficStation.mainTable):
                    logging.info(f'Precess complete with: {traffic_station_files_to_read}')
                    for file in traffic_station_files_to_read:
                        self.dbController.insert('files', [file])
                else:
                    logging.error(f'Can not process all tables as one main table')

            else:
                logging.info('All files has been read')

        except Exception as e:
            logging.error('Nothing new to read in: {0}-{1}'.format(self.trafficDataController.pathName, e))
