#! usr/bin/env python3

import pandas as pd
import file_reader as fr
import sys
import logging
import os


class ClimateFileReader(fr.FileReader):
    """Esta clase contiene las funciones y la información necesaria para realizarlectura de los archivos .csv que
     esxitan dentro de la carpeta resources realcionados con el clima"""

    def __init__(self, rutaArchivo, extencion):
        logging.info('Creating Climate file reader object')
        fr.FileReader.__init__(self, rutaArchivo, extencion)

    def load_files(self, cola='', files_to_read=''):
        """Esta función se encarga de leer todos los archivos en el directorio que cumplen
        con el criterio de la extención"""
        logging.info(f'Start loading files using process: {os.getpid()}')
        temp_table = []
        for file in files_to_read:
            logging.info('Cargando tabla desde: ' + str(self.pathName) + str(file))
            try:
                if '.csv' in file:
                    logging.info('New .csv file detected: ' + str(file))
                    temp_table.append(self.__read_csv_data(self.pathName, file))

                elif '.txt' in file:
                    logging.info('New .txt file detected: ' + str(file))
                    temp_table.append(self.__read_txt_files(self.pathName, file))

            except Exception as e:
                logging.error("Exception: El archivo '{0}' no pudo ser cargado ('{1} - {2}')".format(file,
                                                                                                     e,
                                                                                                     e.__traceback__.tb_lineno))
                continue

        try:
            # Uncomment this when activate multiprocessing
            for tabl in temp_table:
                cola.put([tabl, os.getpid()])

            # self.mainTable = pd.concat(temp_table)
            # self.columnNames = self.mainTable.columns
            return True

        except Exception as e:
            logging.error("Exception: can't concatenate tables - {0}- {1}".format(e, e.__traceback__.tb_lineno))
            return False

    def __read_csv_data(self, path_name, file_name):
        strfile = path_name + file_name
        try:
            logging.info('Reading data: {}'.format(file_name))
            datos = pd.read_csv(strfile, sep=';')
            # Separar la información contenida en la columna MAGNITUD en sus tres
            # componentes.
            tmp = pd.DataFrame(datos['PUNTO_MUESTREO'])
            tmp['TECNICA_MUESTREO'] = 0
            for fila in range(0, len(tmp)):
                tmp.iloc[fila, 1] = str.split(tmp.iloc[fila, 0], "_")[2]
                tmp.iloc[fila, 0] = str.split(tmp.iloc[fila, 0], "_")[0]

            # Se asignan las columnas nuevas al DataFrame general.
            datos['PUNTO_MUESTREO'] = tmp['PUNTO_MUESTREO']
            datos.insert(5, 'TECNICA_MUESTREO', tmp['TECNICA_MUESTREO'])
            logging.info(f'File: {file_name} read - Original length: {str(len(datos))}')

        except Exception as e:
            logging.error("Error: {0} - {1}".format(e, e.__traceback__.tb_lineno))

        return self.__prepare_data(datos, file_name)

    def __prepare_data(self, datos, file_name):
        logging.info('Reshaping data: {}'.format(file_name))
        datos = datos.astype({'MAGNITUD': int})
        datos = datos[(datos.MAGNITUD == 6)]

        # Preparación de la tabla de datos
        col_names = datos.columns
        col_comunes = list(col_names[0:9])
        col_names_1 = list(col_names[9::2])
        col_names_2 = list(col_names[10::2])

        data_1 = datos[col_comunes + col_names_1]
        data_2 = datos[col_comunes + col_names_2]

        data_long_1 = data_1.melt(id_vars=col_comunes,
                                  var_name='HORA',
                                  value_name='VALOR')

        data_long_2 = data_2.melt(id_vars=col_comunes,
                                  var_name='HORA',
                                  value_name='VALIDEZ')

        data_long_1['HORA'] = data_long_1.HORA.str.replace('H', '')
        data_long_2['HORA'] = data_long_2.HORA.str.replace('V', '')

        # Operación de merge con la que se obtiene la tabla final para comerzar
        # a realizar las operaciones.
        data_3 = data_long_1.merge(data_long_2, how='inner', on=col_comunes + ['HORA'])

        # Pasar la hora formato datetime
        data_3 = data_3.astype({'HORA': int, 'PUNTO_MUESTREO': int})
        data_3.HORA = data_3.HORA - 1

        # Sea agrega una columna que puede servir como indice.
        data_3['TIMESTAMP'] = 0

        data_3['TIMESTAMP'] = data_3.apply(lambda fila: self.__process_timestamp(fila), axis=1)

        # Reorganizar las columnas para a justarse a el modelo de la base de datos
        data_3 = self.__reorder_columns(data_3)

        logging.info('Done reshaping data: {} - Data shape: {}'.format(file_name, str(data_3.shape)))
        return data_3

    def __read_txt_files(self, path_name, file_name):
        """Esta función se encarga de leer los archiivos de tipo .txt en el repositorio de calidad del aire y
        prepararlos para la carga"""
        logging.info('Reading and process data...')
        try:
            str_file_path = path_name + file_name
            with open(str_file_path, 'r') as datos:
                contador = 0
                splitters = [2, 5, 8, 10, 12, 14, 16, 18, 20, 22]
                lst = []
                while True:
                    temporal_row = []
                    _ = datos.readline()
                    if not _:
                        logging.info('Done: ' + str(contador) + ' processed')
                        break

                    for spl in range(0, len(splitters)):
                        if spl == 0:
                            temporal_row.append(_[0:splitters[spl]])

                        else:
                            if spl == 3:
                                # print('spl = ', str(spl))
                                concat_str = temporal_row[0] + temporal_row[1] + temporal_row[2]
                                temporal_row.append(_[splitters[spl - 1]:splitters[spl]])
                                temporal_row.append(concat_str)

                            elif spl == len(splitters) - 1:
                                # print('spl = ', str(spl))
                                tmp_values = _[splitters[spl - 1]:]

                                for i in range(1, 25):
                                    if i == 1:
                                        v = tmp_values[0:(i * 6)]
                                        temporal_row.append(v[0:5])
                                        temporal_row.append(v[5:6])

                                    else:
                                        v = tmp_values[((i - 1) * 6):(i * 6)]
                                        temporal_row.append(v[0:5])
                                        temporal_row.append(v[5:6])

                            elif spl == 6:
                                # print('spl = ', str(spl), _[splitters[spl - 1]:splitters[spl]])
                                temporal_row.append('20' + _[splitters[spl - 1]:splitters[spl]])

                            else:
                                # print('spl = ', str(spl), _[splitters[spl - 1]:splitters[spl]], '*')
                                temporal_row.append(_[splitters[spl - 1]:splitters[spl]])

                    temporal_row.pop(6)
                    contador += 1
                    lst.append(temporal_row)

        except Exception as e:
            logging.error('Exception: {}'.format(e))

        try:
            column_names = ['PROVINCIA', 'MUNICIPIO', 'ESTACION', 'MAGNITUD', 'PUNTO_MUESTREO', 'TECNICA_MUESTREO',
                            'ANO', 'MES', 'DIA',
                            'H01', 'V01', 'H02', 'V02', 'H03', 'V03', 'H04', 'V04', 'H05', 'V05', 'H06', 'V06', 'H07',
                            'V07', 'H08', 'V08', 'H09', 'V09', 'H10', 'V10', 'H11', 'V11', 'H12', 'V12', 'H13', 'V13',
                            'H14', 'V14', 'H15', 'V15', 'H16', 'V16', 'H17', 'V17', 'H18', 'V18', 'H19', 'V19', 'H20',
                            'V20', 'H21', 'V21', 'H22', 'V22', 'H23', 'V23', 'H24', 'V24']

            final_data = pd.DataFrame(lst, columns=column_names)
            final_data = final_data.astype({'MES': int, 'DIA': int})

        except Exception as e:
            logging.error('Exception: {0} - {1} - {2}'.format(e, e.__traceback__.tb_frame, e.__traceback__.tb_lineno))

        return self.__prepare_data(final_data, file_name)

    def __process_timestamp(self, fila):
        # return pd.datetime(fila.ANO, fila.MES, fila.DIA)
        return int('0'.join([str(fila.ANO), str(fila.MES), str(fila.DIA)]))

    def __reorder_columns(self, table):
        table = table[['PUNTO_MUESTREO', 'TIMESTAMP', 'HORA', 'MAGNITUD', 'VALOR', 'VALIDEZ']]
        return table


if __name__ == '__main__':
    logging.basicConfig(level='DEBUG',
                        filename='log.txt',
                        filemode='a',
                        format='%(asctime)s;%(levelname)s;%(name)s;%(module)s;%(funcName)s;%(message)s')

    c = ClimateFileReader(rutaArchivo='resources/calidad_aire_madrid/datos_horarios_anos_2001_2019/', extencion='.csv')
    for name, valor in c.__dict__.items():
        print('{0}: {1}'.format(name, valor))

    c.load_files()
    print(c.mainTable)
    print(sys.getsizeof(c.mainTable))

    for name, valor in c.__dict__.items():
        print('{0}: {1}'.format(name, valor))
