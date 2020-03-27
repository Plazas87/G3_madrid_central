#! usr/bin/env python3

import pandas as pd
import logging
import file_reader as fr

from enums import StationTable


class ClimateStation(fr.FileReader):
    def __init__(self, rutaArchivo, extencion):
        logging.info('Creating Climate station reader object')
        fr.FileReader.__init__(self, rutaArchivo, extencion)

    def load_files(self, cola='', files_to_read=''):
        try:
            datos = pd.read_csv(self.pathName + files_to_read,
                                sep=';',
                                header='infer',
                                encoding='iso-8859-1')

            datos = datos[['CODIGO', 'ESTACION', 'COD_TIPO', 'DIRECCION', 'LATITUD', 'LONGITUD', 'ALTITUD', 'Fecha alta']]
            datos.columns = [StationTable.station_id.name, StationTable.name.name, StationTable.type.name,
                             StationTable.address.name, StationTable.latitude.name, StationTable.longitude,
                             StationTable.altitude.name, StationTable.start_date.name]

        except Exception as e:
            logging.error("Can't read the station table: {0}".format(e))
            return False

        self.mainTable = datos
        return True



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

