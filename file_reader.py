#! usr/bin/env python3

import os
import pandas as pd
import logging


class FileReader:
    def __init__(self, rutaArchivo, extencion):
        self.pathName = rutaArchivo
        self.fileExtension = extencion
        self.files = []
        self.totalFiles = 0
        self.mainTable = []
        self.columnNames = []
        self.errorFiles = []

    def load_files(self, cola='', files_to_read=''):
        """Esta función se encarga de leer todos los archivos en el directorio que cumplen
        con el criterio de la extención"""
        logging.info('Start loading files')
        temp_table = []
        for file in self.files:
            print('Cargando tabla desde: ' + self.pathName + file)
            try:
                datos = pd.read_csv(self.pathName + file, sep=';', encoding='iso-8859-1')
                temp_table.append(datos)
                logging.info("{} Load success".format(file))
            except Exception as e:
                print("Execption: can't load file :  {}".format(file))
                logging.error("Exception: {}".format(e))

        self.mainTable = pd.concat(temp_table, sort=True)
        self.columnNames = list(self.mainTable.columns)

    def search_for_files(self):
        """Esta función se encarga de extraer el nombre de todos los archivos existentes en un directorio dado"""
        logging.info("Looking for files in : {}".format(self.pathName))
        self.files = []
        file_temp = []
        for r, d, f in os.walk(self.pathName):
            file_temp = f

        logging.info('Files found: ' + str(file_temp))
        for file in file_temp:
            for ext in self.fileExtension:
                if ext in file:
                    self.files.append(file)

        logging.info("Total valid files : {0}".format(self.files))
        self.totalFiles = len(self.files)

    def __str__(self):
        return '- Ruta de archivo: ' + self.pathName + '\n' + '- Extención de archivos: ' + self.fileExtension + '\n' + \
               '- Archivos: ' + str(self.files) + '\n' + '-Andres: ' + str(self.totalFiles)


if __name__ == '__main__':
    lectura = FileReader('resources/trafico/ubicacion_puntos_medicion_trafico/', ['.csv'])




