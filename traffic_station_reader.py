#! usr/bin/env python3


import logging
import file_reader as fr
import pandas as pd


class TrafficStation(fr.FileReader):
    def __init__(self, rutaArchivo, extencion):
        logging.info('Creating traffic station reader object')
        fr.FileReader.__init__(self, rutaArchivo, extencion)

    def load_files(self, cola='', files_to_read=''):
        """Esta función se encarga de leer todos los archivos en el directorio que cumplen
        con el criterio de la extención"""
        logging.info('Start loading files')
        temp_table = []
        for file in files_to_read:
            print('Cargando tabla desde: ' + self.pathName + file)


            # try:
            #     datos = pd.read_csv(self.pathName + file, sep=';', encoding='iso-8859-1')
            #     temp_table.append(datos)
            #     logging.info("{} Load success".format(file))
            # except Exception as e:
            #     print("Execption: can't load file :  {}".format(file))
            #     logging.error("Exception: {}".format(e))

        self.mainTable = pd.concat(temp_table, sort=True)
        self.columnNames = list(self.mainTable.columns)

    def prepare_data(self):
        data = self.mainTable[["id", "distrito", "nombre", "tipo_elem", "latitud", "longitud"]]
        # data = data[(data["tipo_elem"] == "URB")]
        data.columns = ['station_id', "district", 'address', 'type', 'latitude', 'longitude']
        data = data.fillna(0, axis=0)
        self.mainTable = data


if __name__ == '__main__':

    # obj = TrafficStation()
    # print(obj)

    obj = TrafficStation('resources/trafico/ubicacion_puntos_medicion_trafico/', ['.csv'])
    for key, value in obj.__dict__.items():
        print(f'Key: {key} - Value: {value}')

    print('*******************************')

    for key, value in TrafficStation.__dict__.items():
        print(f'Key: {key} - Value: {value}')




    # lectura_estacion_trafico = TrafficStation('resources/trafico/ubicacion_puntos_medicion_trafico/', ['.csv'])
    # lectura_estacion_trafico.search_for_files()
    # lectura_estacion_trafico.load_files()
    # lectura_estacion_trafico.prepare_data()
    # print('Fin')
    # print()

