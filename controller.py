from climate_file_reader import ClimateFileReader
from climate_station_reader import ClimateStation
from traffic_file_reader import TrafficFileReader
from traffic_station_reader import TrafficStation
from db_controller import DatabaseController
import logging
import sys


class Controller:
    """Controlador que se encanga de armonizar la funcionalidades de todos los objetos de las clases presentes """
    def __init__(self, configuration):
        logging.info('Starting controller')
        self.dbController = DatabaseController(configuration)
        self.airQualityDataController = ClimateFileReader(rutaArchivo=configuration.air_quality_path,
                                                          extencion=['.txt', '.csv'])

        self.airQualityStation = ClimateStation(rutaArchivo=configuration.air_stations_path,
                                                extencion=['.csv'])

        self.trafficDataController = TrafficFileReader(rutaArchivo=configuration.traffic_data_path,
                                                       extencion=['.csv'])

        self.trafficStation = TrafficStation(rutaArchivo=configuration.traffic_station_path,
                                             extencion=['.csv'])

    def __check_files(self):
        logging.info('Look for files that has been already loaded')
        query = self.dbController.selectQuery('files', 'file_name', info='Checking for files in files table')
        print('Previously read files', query)
        logging.info('Previously read files: ' + str(query))

        return query

    def __check_files_by_path(self, query, obj_files, path):
        logging.info('Checkin files in: {}'.format(path))
        files_read = []
        try:
            # Comprueba que archivos an sido leidos previamente, y por lo tanto no se deben volver a leer
            for i in obj_files:
                if i in query:
                    logging.info('File: ' + str(i) + ' Already loaded')
                    print('No se carga')
                    pass
                else:
                    print(i, 'Se carga')
                    files_read.append(i)
                    print('Controller ******')

        except Exception as e:
            logging.error("Can't read files from database " + str(e))

        return files_read

    def read_data(self):
        """Esta función se encarga de leer los archivos que se encuentran en cada una de las ubicaciones del repositorio,
        si el archivo ya ha sido leido, no se carga nuevaente, de lo contrario el archivo se carga normalmente."""
        try:
            load_files = self.__check_files()
            # Cargar data de calidad del aire
            files_to_read = self.__check_files_by_path(load_files,
                                                       self.airQualityDataController.files,
                                                       self.airQualityDataController.pathName)

            if len(files_to_read) != 0:
                self.airQualityDataController.load_files(files_to_read)
                print(self.airQualityDataController.columnNames)
                self.dbController.insert('files', files_to_read,
                                         'Insert files from {} into files table'.format(self.airQualityDataController.pathName))

            else:
                logging.info('Nothing new to read in: {}'.format(self.airQualityDataController.pathName))

            # read files from the air quality station path
            files_to_read = self.__check_files_by_path(load_files,
                                                       self.airQualityStation.files,
                                                       self.airQualityStation.pathName)
            if len(files_to_read) != 0:
                self.airQualityStation.load_files()
                self.dbController.insert('files', files_to_read,
                                         'Insert files from {} into file table'.format(self.airQualityStation.pathName))

            else:
                logging.info('Nothing new to read in {} '.format(self.airQualityStation.pathName))

                # read files from traffic flow path
                files_to_read = self.__check_files_by_path(load_files,
                                                           self.trafficDataController.files,
                                                           self.trafficDataController.pathName)
                if len(files_to_read) != 0:
                    self.trafficDataController.load_files()
                    self.dbController.insert('files', files_to_read,
                                             'Insert files from {} into file table'.format(
                                                 self.trafficDataController.pathName))

                else:
                    logging.info('Nothing new to read in {} '.format(self.trafficDataController.pathName))

                    # read files from traffic flow station path
                    files_to_read = self.__check_files_by_path(load_files,
                                                               self.trafficStation.files,
                                                               self.trafficStation.pathName)
                    if len(files_to_read) != 0:
                        self.trafficStation.load_files()
                        self.dbController.insert('files', files_to_read,
                                                 'Insert files from {} into file table'.format(
                                                     self.trafficStation.pathName))

                    else:
                        logging.info('Nothing new to read in {} '.format(self.trafficStation.pathName))

            # files_to_read = self.__check_files_by_path(load_files, self.trafficDataController.files)
            # self.trafficDataController.load_files(files_to_read)
            #
            # files_to_read = self.__check_files_by_path(load_files, self.trafficDataController.files)
            # self.trafficDataController.load_files(files_to_read)
            #
            # files_to_read = self.__check_files_by_path(load_files, self.trafficStation.files)
            # self.trafficStation.load_files(files_to_read)


            # if fil in self.airQualityDataController.files:
            #     _.append(fil)
            #     self.airQualityDataController.load_files(_)
            #     # _ =[]
            #
            # if fil in self.airQualityStation.files:
            #     _.append(fil)
            #     self.airQualityStation.load_files(_)
            #     # _ = []
            #
            # if fil in self.trafficDataController.files:
            #     _.append(fil)
            #     self.trafficDataController.load_files(_)
            #     # _ = []
            #
            # if fil in self.trafficStation.files:
            #     _.append(fil)
            #     self.trafficStation.load_files(_)
            #     # _ = []
            #
            # _ = []

        except Exception as e:
            print(e)

        # if len(load_files) != 0:
        #     self.dbController.insert('files', load_files)
        #     pass

        # readed_files = self.check_files()
        # files_to_read = []
        # temp_object_list = []
        # try:
        #     # Comprueba que archivos an sido leidos previamente, y por lo tanto no se deben volver a leer
        #     query = self.dbController.selectQuery('files', 'file_name')
        #     print('Previously read files', query)
        #     logging.info('Previously read files: ' + str(query))
        #     if len(query) == 0 or query is None:
        #         logging.info('Loading files for the firs time')
        #         self.airQualityDataController.load_files(self.airQualityDataController.files)
        #         self.dbController.insert('madridcentral', 'files', self.airQualityDataController.files)
        #
        #     else:
        #         for i in self.airQualityDataController.files:
        #             if i in query:
        #                 logging.info('File: ' + str(i) + ' Already loaded')
        #                 pass
        #             else:
        #                 print(i, 'Se carga')
        #                 files_to_read.append(i)
        #                 logging.info('Loading new files: ' + str(files_to_read))
        #                 self.airQualityDataController.load_files(files_to_read)
        #                 self.dbController.insert('files', files_to_read)
        #                 print('Controller')
        #
        # except Exception as e:
        #     print(e)

    def start(self):
        self.read_data()



        # try:
        #     query = self.dbController.selectQuery('files', 'file_name')
        #     print(query)
        #     if query is not None or len(query) != 0:
        #         for row in self.airQualityDataController.files:
        #             if row not in query:
        #                 print('TRUE')
        #                 files_to_read.append(row)
        #
        #         self.airQualityDataController.load_files(files_to_read)
        #
        #     else:
        #         self.airQualityDataController.load_files(self.airQualityDataController.files)
        #
        #     print(files_to_read, '***')
        #
        #     self.dbController.insert('files', self.airQualityDataController.files)
        #
        # except Exception as e:
        #     logging.error('Error - ' + str(e))
        #
        # try:
        #     self.airQualityStation.load_files()  # devuelve algo esta función?
        #
        # except Exception as e:
        #     logging.error('Error while loading file - ' + str(e))
        #
        # try:
        #     self.trafficStation.load_files()  # devuelve algo esta función?
        #
        # except Exception as e:
        #     logging.error('Error while loading file - ' + str(e))
        #
        # print(sys.getsizeof(self.airQualityDataController.mainTable))
        # print(sys.getsizeof(self.airQualityStation.mainTable))
        # print(sys.getsizeof(self.trafficStation.mainTable))

        #
        # for name, valor in airQualityDataController.__dict__.items():
        #     print('{0}: {1}'.format(name, valor))


        # # Se hace el merge de la tabla de datos con la tabla de estaciones
        # final = data_3.merge(estaciones_medicion, on='PUNTO_MUESTREO', how='inner')
        #
        # # Se agrega la columna timestamp por si sirve en el futuro como index
        # final['Timestamp'] = 0
        # def process_timestamp(fila):
        #     return pd.datetime(fila.ANO, fila.MES, fila.DIA, fila.HORA)
        #
        # final['Timestamp'] = final.apply(lambda fila: process_timestamp(fila), axis=1)
        #
        # # Diccionario de python que almacena los datos para cada fecha y hora en cada
        # # una de las estaciones.
        # final_data_estaciones = {}
        #
        # #for name, group in final.groupby(['ANO', 'MES', 'DIA', 'HORA']):
        # #    final_data_estaciones[name] = group
        #
        #
        # for name, group in final.groupby(['ANO', 'MES', 'DIA', 'HORA']):
        #     final_data_estaciones[name] = group
        #
        # # pd.date_range(start='2017-01-01', end='2017-01-04', freq='1H')
        #
        #


