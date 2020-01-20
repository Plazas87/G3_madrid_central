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
        self.dbContrller = DatabaseController(configuration)
        self.airQualityDataController = ClimateFileReader(rutaArchivo=configuration.air_quality_path, extencion='.csv')
        self.airQualityStation = ClimateStation(rutaArchivo=configuration.air_stations_path, extencion='.csv')
        self.trafficDataController = TrafficFileReader(rutaArchivo=configuration.traffic_data_path, extencion='.csv')
        self.trafficStation = TrafficStation(rutaArchivo=configuration.traffic_station_path, extencion='.csv')

    def read_data(self):
        try:
            self.airQualityDataController.load_files()  # devuelve algo esta función?
            self.dbContrller.insert('files', self.airQualityDataController.files)

        except Exception as e:
            logging.error('Error - ' + str(e))

        try:
            self.airQualityStation.load_files()  # devuelve algo esta función?

        except Exception as e:
            logging.error('Error while loading file - ' + str(e))

        try:
            self.trafficStation.load_files()  # devuelve algo esta función?

        except Exception as e:
            logging.error('Error while loading file - ' + str(e))

        print(sys.getsizeof(self.airQualityDataController.mainTable))
        print(sys.getsizeof(self.airQualityStation.mainTable))
        print(sys.getsizeof(self.trafficStation.mainTable))

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


