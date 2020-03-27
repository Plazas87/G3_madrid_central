#! usr/bin/env python3

import pandas as pd
import logging
import file_reader as fr


class TrafficFileReader(fr.FileReader):
    """Esta clase contiene las funciones y la información necesaria para realizarlectura de los archivos .csv que
     esxitan dentro de la carpeta resources realcionados con el clima"""
    def __init__(self, rutaArchivo, extencion):
        logging.info('Creating traffic data reader object')
        fr.FileReader.__init__(self, rutaArchivo, extencion)


if __name__ == '__main__':
    path_name = 'resources/trafico/trafico_historico_desde_2013/'
    file_name = '10-2019.csv'
    c = TrafficFileReader(path_name, file_name)
    c.load_files()
