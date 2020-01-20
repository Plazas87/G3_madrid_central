#! usr/bin/env python3

import pandas as pd
import logging
import file_reader as fr


class ClimateStation(fr.FileReader):
    def __init__(self, rutaArchivo, extencion):
        logging.info('Creating Climate station reader object')
        fr.FileReader.__init__(self, rutaArchivo, extencion)


if __name__ == '__main__':
    logging.basicConfig(level='DEBUG',
                        filename='log.txt',
                        filemode='a',
                        format='%(asctime)s;%(levelname)s;%(name)s;%(module)s;%(funcName)s;%(message)s')

    station = ClimateStation()
    print(station.__dict__)
    station.load_files()
    print(station)
    print(station.mainTable)

