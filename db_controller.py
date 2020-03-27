#! usr/bin/env python3

import sqlite3
import psycopg2 as db
import logging
import pandas as pd
from datetime import datetime
import numpy as np
from built_configuration import BuildConfiguration
from enums import StationTable
from enums import TimeTable
from enums import MeasurementTable
from enums import TrafficcMeasurement
from enums import TrafficStation
from enums import Traffic2Table
from configparser import ConfigParser
from enum import Enum


class DatabaseController:
    # Singleton implementation
    """Clase que controla la conexión a la base de datos. Esta implementación asegura que únicamente exista un objeto
    de esta clase, es decir, una sola conexión a la base de datos."""
    def __init__(self, configuration):
        logging.info('Starting database controller')
        self.user = configuration.user
        self.__password = configuration.password
        self.address = configuration.address
        self.port = configuration.port
        self.database = configuration.database
        self.schema = ''

    def __new__(cls, name=None, params=None):
        if not hasattr(cls, 'instance'):  # Si no existe el atributo “instance”
            cls.instance = super(DatabaseController, cls).__new__(cls)  # lo creamos
        return cls.instance

    def connect(self, process_information='put some here'):
        try:
            # connect to the PostgreSQL server
            conn = None
            conn = db.connect(user=self.user,
                              password=self.__password,
                              host=self.address,
                              port=self.port,
                              database=self.database)
            return conn
        except Exception as e:
            logging.error('Unable to connect' + str(e))
            return None

    def close_connection(self, connection):
        """Termina la conexión con la base de datos. Esta función de ser llamada siempre despues de cualquier
        operación en la base de datos"""
        try:
            connection.close()
        except Exception as e:
            logging.error('Unable to close the connection: {}'.format(e))
            return None

    def insert(self, table, data, info=' '):
        # Crea la conexión con la base de datos
        conn = self.connect(process_information=info)
        if conn is not None:
            if table == 'files':
                try:
                    query = "INSERT INTO {0} (file_name) VALUES (%s)".format(table)
                    cursor = conn.cursor()
                    for value in data:
                        cursor.execute(query, (value,))
                        print('Almacenada correctamente en la base de datos')

                    conn.commit()

                except Exception as e:
                    print(e)
                    cursor.close()
                    self.close_connection(conn)
                    print("PostgreSQL connection has been closed but an Exception has been raised")
                    return None
                else:
                    cursor.close()
                    self.close_connection(conn)
                    print("PostgreSQL connection is closed")
                    return query

            if table == 'day':
                try:
                    query = "INSERT INTO {0} VALUES (%s, %s, %s, %s)".format(table)
                    cursor = conn.cursor()
                    cursor.execute(query, (data[0], data[3], data[2], data[1]))
                    print('La orden ha sido almacenada correctamente en la base de datos')

                    conn.commit()

                except Exception as e:
                    print(e)
                    cursor.close()
                    self.close_connection(conn)
                    print("PostgreSQL connection has been closed but an Exception has been raised")
                    return None
                else:
                    cursor.close()
                    self.close_connection(conn)
                    print("PostgreSQL connection is closed")
                    return query

            if table == 'time':
                try:
                    query = "INSERT INTO {0} VALUES (%s, %s)".format(table)
                    cursor = conn.cursor()
                    cursor.execute(query, (data[TimeTable.time_id.value], data[TimeTable.hour.value]))
                    print('La orden ha sido almacenada correctamente en la base de datos')

                    conn.commit()

                except Exception as e:
                    print(e)
                    cursor.close()
                    self.close_connection(conn)
                    print("PostgreSQL connection has been closed but an Exception has been raised")
                    return None
                else:
                    cursor.close()
                    self.close_connection(conn)
                    print("PostgreSQL connection is closed")
                    return query

            if table == 'station':
                try:
                    query = "INSERT INTO {0} VALUES (%s, %s, %s, %s, %s, %s, %s, %s)".format(table)
                    cursor = conn.cursor()
                    error_row = []
                    for row in data.values:
                        try:
                            tmp_row = list(row)
                            cursor.execute(query, (tmp_row[StationTable.station_id.value],
                                                   tmp_row[StationTable.name.value],
                                                   tmp_row[StationTable.type.value],
                                                   tmp_row[StationTable.address.value],
                                                   tmp_row[StationTable.latitude.value],
                                                   tmp_row[StationTable.longitude.value],
                                                   tmp_row[StationTable.altitude.value],
                                                   tmp_row[StationTable.start_date.value]))
                        except Exception as e:
                            logging.error("Can't insert row: {}".format(e))
                            error_row.append(row)
                            continue

                except Exception as e:
                    logging.error('Error: PostgreSQL connection has been closed but an Exception has been raised - {0}'.format(e))
                    cursor.close()
                    self.close_connection(conn)
                    return False
                else:
                    conn.commit()
                    cursor.close()
                    self.close_connection(conn)
                return True

            if table == 'measurement':
                try:
                    query = "INSERT INTO {0} VALUES (%s, %s, %s, %s, %s, %s)".format(table)
                    cursor = conn.cursor()
                    logging.info(f'Lenght data to databaase : {len(data)}')
                    error_row = []
                    for row in data.values:
                        try:
                            tmp_row = list(row)
                            cursor.execute(query, (tmp_row[MeasurementTable.station_id.value],
                                                   tmp_row[MeasurementTable.day_id.value],
                                                   tmp_row[MeasurementTable.time_id.value],
                                                   tmp_row[MeasurementTable.magnitude_id.value],
                                                   tmp_row[MeasurementTable.value.value],
                                                   tmp_row[MeasurementTable.validation.value]))
                        except Exception as e:
                            logging.error('Cant insert row: {}'.format(tmp_row))
                            error_row.append(row)
                            continue

                except Exception as e:
                    logging.error('Error: PostgreSQL connection has been closed but an Exception has been raised - {0}'.format(e))
                    cursor.close()
                    self.close_connection(conn)
                    return False
                else:
                    conn.commit()
                    cursor.close()
                    self.close_connection(conn)
                return True, error_row

            if table == 'traffic':
                try:
                    query = "INSERT INTO {0} VALUES (%s, %s, %s, %s, %s, %s)".format(table)
                    cursor = conn.cursor()
                    logging.info(f'Lenght data to databaase : {len(data)}')
                    error_row = []
                    for row in data.values:
                        try:
                            tmp_row = list(row)
                            cursor.execute(query, (tmp_row[TrafficcMeasurement.station_id.value],
                                                   tmp_row[TrafficcMeasurement.day_id.value],
                                                   tmp_row[TrafficcMeasurement.time_id.value],
                                                   tmp_row[TrafficcMeasurement.intensity.value],
                                                   tmp_row[TrafficcMeasurement.avg_speed.value],
                                                   tmp_row[TrafficcMeasurement.validation.value]))
                        except Exception as e:
                            logging.error('Cant insert row: {0} - {1}'.format(tmp_row, str(e)))
                            error_row.append(row)
                            continue

                except Exception as e:
                    logging.error('Error: PostgreSQL connection has been closed but an Exception has been raised - {0}'.format(e))
                    cursor.close()
                    self.close_connection(conn)
                    return False
                else:
                    conn.commit()
                    cursor.close()
                    self.close_connection(conn)
                return True, error_row

            if table == 'traffic2':
                try:
                    # query = "INSERT INTO {0} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(table)
                    cursor = conn.cursor()
                    logging.info(f'Python al mando : {data}')
                    queries = []

                    query_step_one = "COPY traffic2 FROM 'D:/Camilo/Documentos/Masters/EAE/Master en Big Data & Analytics/Trabajo final de Master/backup resources/main_directori_tfm/resources/trafico/trafico_historico_desde_2013/01-2016.csv'\
                    DELIMITER ',' CSV HEADER"
                    queries.append(query_step_one)

                    query_step_two = "CREATE VIEW vista AS\
                                       select station_id, EXTRACT(YEAR from fecha) as ano, EXTRACT(MONTH from fecha) as mes, EXTRACT(DAY from fecha) as dia, EXTRACT(HOUR from fecha) as time_id, 'intensity' as magnitude, sum(intensidad) as value, error_ as validation from traffic2\
                                       group by station_id, ano, mes, dia, time_id, validation"
                    queries.append(query_step_two)

                    query_step_three = "create view vista2 as\
                                        select station_id, to_number(CONCAT(to_char(ano, '9999'), '0', to_char(mes, '99'), '0', to_char(dia, '99')), '9999999999') as day_id, time_id, 'intensity' as magnitude, value, validation from vista"
                    queries.append(query_step_three)

                    for query in queries:
                        cursor.execute(query)

                except Exception as e:
                    logging.error('Error: PostgreSQL connection has been closed but an Exception has been raised - {0}'.format(e))
                    cursor.close()
                    self.close_connection(conn)
                    return False
                else:
                    conn.commit()
                    cursor.close()
                    self.close_connection(conn)

            if table == 'trafficstation':
                try:
                    query = "INSERT INTO {0} VALUES (%s, %s, %s, %s, %s, %s)".format(table)
                    cursor = conn.cursor()
                    logging.info(f'Lenght data to databaase : {len(data)}')
                    error_row = []
                    for row in data.values:
                        try:
                            tmp_row = list(row)
                            cursor.execute(query, (tmp_row[TrafficStation.station_id.value],
                                                   tmp_row[TrafficStation.distric.value],
                                                   tmp_row[TrafficStation.address.value],
                                                   tmp_row[TrafficStation.type_station.value],
                                                   tmp_row[TrafficStation.latitude.value],
                                                   tmp_row[TrafficStation.longitude.value]))
                        except Exception as e:
                            logging.error('Cant insert row: {0} - {1}'.format(tmp_row, str(e)))
                            error_row.append(row)
                            continue

                except Exception as e:
                    logging.error('Error: PostgreSQL connection has been closed but an Exception has been raised - {0}'.format(e))
                    cursor.close()
                    self.close_connection(conn)
                    return False

                else:
                    conn.commit()
                    cursor.close()
                    self.close_connection(conn)
                    return True

    def selectQuery(self, table_name, *columns, filter_table=None, info=' '):
        """Este método se encarga de realizar las consultas a todas las tablas de la base de datos del proyecto. Es lo
        suficientemente versatil como para entender varios tipos de consultas a las diferentes tablas"""
        query = []
        cursor = ''
        conn = ''
        str_query = ''
        if table_name == 'files':
            if columns[0] == 'file_name' and filter_table is None:
                str_query = "SELECT {0} FROM {1}".format(columns[0], table_name)

        if str_query != '':
            try:
                conn = self.connect(process_information=info)
                if conn is not None:
                    cursor = conn.cursor()
                    cursor.execute(str_query)
                    qu = list(cursor.fetchall())
                    for i in qu:
                        query.append(str(i[0]))
            except Exception as e:
                print(e)
                cursor.close()
                self.close_connection(conn)
                logging.info('PostgreSQL connection has been closed but an Exception has been raised')
                return None
            else:
                cursor.close()
                self.close_connection(conn)
                logging.info('PostgreSQL connection is closed')
                return query

        else:
            return None


if __name__ == '__main__':
    config = BuildConfiguration()
    dbController = DatabaseController(config)

    # script para llenar la tabla day
    # date_rng = list(pd.date_range(start='2016/01/01', end='2019/10/31', freq='D'))
    # tmp = []
    #
    # for fila in date_rng:
    #     tmp.append(str(fila).split(' ')[0].split('-'))
    #
    # print(tmp)
    #
    # for i in range(0, len(tmp)):
    #     for j in range(0, len(tmp[i])):
    #         tmp[i][j] = str(int(tmp[i][j]))
    #
    # print(tmp)
    #
    # for dat in tmp:
    #     key = '0'.join(dat)
    #     dat.insert(0, int(key))
    #     # print(dat, '*****')
    #     dbController.insert('day', dat)
    #
    # print('finish')
    # Fin

    # scrip para llenar la tabla time
    # for i in range(0, 24):
    #     time_tmp = [i, i]
    #     dbController.insert('time', time_tmp)
    #     print('finish')
    # #     FIN
    #
    # script para llenar la tabla station
    # datos = pd.read_csv(
    #     'resources/calidad_aire_madrid/informacion_estaciones_red_calidad_aire/informacion_estaciones_red_calidad_aire.csv',
    #     sep=';',
    #     header='infer',
    #     encoding='iso-8859-1')
    #
    # datos = datos[['CODIGO', 'ESTACION', 'COD_TIPO', 'DIRECCION', 'LATITUD', 'LONGITUD', 'ALTITUD', 'Fecha alta']]
    # print(datos)
    # datos.columns = [StationTable.station_id.name, StationTable.name.name, StationTable.type.name,
    #                  StationTable.address.name, StationTable.latitude.name, StationTable.longitude,
    #                  StationTable.altitude.name, StationTable.start_date.name]
    #
    # for i in datos.values:
    #     print(list(i))
    #     dbController.insert('station', list(i), 'insert station data')
    #     print('finish')

    # FIN











