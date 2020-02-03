#! usr/bin/env python3


import logging
import file_reader as fr


class TrafficStation(fr.FileReader):
    def __init__(self, rutaArchivo, extencion):
        logging.info('Creating traffic station reader object')
        fr.FileReader.__init__(self, rutaArchivo, extencion)


if __name__ == '__main__':
    station = ClimateStation()
    print(station.__dict__)
    station.load_files()
    print(station)
    print(station.mainTable)
    for name, valor in station.__dict__.items():
        print('{0}: {1}'.format(name, valor))


