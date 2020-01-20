import os
import pandas as pd
import logging


class FileReader:
    def __init__(self, rutaArchivo, extencion):
        self.pathName = rutaArchivo
        self.fileExtension = extencion
        self.files = []
        self.totalFiles = self.search_for_files()
        self.mainTable = ''
        self.columnNames = []

    def load_files(self):
        """Esta función se encarga de leer todos los archivos en el directorio que cumplen
        con el criterio de la extención"""
        logging.info('Start loading files')
        temp_table = []
        for file in self.files:
            print('Cargando tabla desde: ' + self.pathName + file)
            try:
                datos = pd.read_csv(self.pathName + file, sep=';', encoding='iso-8859-1')
                temp_table.append(datos)
                logging.info("{} Load succuess".format(file))
            except Exception as e:
                print("Execption: can't load file :  {}".format(file))
                logging.error("Exception: {}".format(e))

        self.mainTable = pd.concat(temp_table, sort=True)
        self.columnNames = list(self.mainTable.columns)

    def search_for_files(self):
        """Esta función se encarga de extraer el nombre de todos los archivos existentes en un directorio dado"""
        logging.info("Looking for files in : {}".format(self.pathName))
        file_temp = []
        for r, d, f in os.walk(self.pathName):
            file_temp = f

        for file in file_temp:
            if self.fileExtension in file:
                self.files.append(file)

        logging.info("Total files : {0}".format(self.files))
        return len(self.files)

    def __str__(self):
        return '- Ruta de archivo: ' + self.pathName + '\n' + '- Extención de archivos: ' + self.fileExtension + '\n' + \
               '- Archivos: ' + str(self.files)
