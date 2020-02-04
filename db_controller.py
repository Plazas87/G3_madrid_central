#! usr/bin/env python3

import sqlite3
import psycopg2 as db
import logging
import pandas as pd
from datetime import datetime
import numpy as np
from built_configuration import BulidConfiguraion
from enums import StationTable
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
        self.schema = configuration.schema

    def __new__(cls, name=None, params=None):
        if not hasattr(cls, 'instance'):  # Si no existe el atributo “instance”
            cls.instance = super(DatabaseController, cls).__new__(cls)  # lo creamos
        return cls.instance

    def connect(self, process_information='put some here'):
        logging.info('Connecting with database: ' + str(process_information))
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
            print('Postgres/Unable to connect -', e, ': ', e.__traceback__.tb_frame)
            logging.error('Unable to connect')
            return None

    def close_connection(self, connection):
        """Termina la conexión con la base de datos. Esta función de ser llamada siempre despues de cualquier
        operación en la base de datos"""
        logging.info('Closing the connection with database')
        try:
            connection.close()
        except Exception as e:
            print('Unable to close the connection -', e, ': ', e.__traceback__.tb_frame)
            logging.error('Unable to close the connection')
            return None

    def insert(self, table, data, info=' '):
        # Crea la conexión con la base de datos
        conn = self.connect(process_information=info)
        if conn is not None:
            if table == 'files':
                try:
                    query = "INSERT INTO {0} (file_name) VALUES (%s)".format('.'.join([self.schema, table]))
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
                    query = "INSERT INTO {0} VALUES (%s, %s, %s, %s)".format('.'.join([self.schema, table]))
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
                    query = "INSERT INTO {0} VALUES (%s, %s)".format('.'.join([self.schema, table]))
                    cursor = conn.cursor()
                    cursor.execute(query, (data[0], data[1]))
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
                    query = "INSERT INTO {0} VALUES (%s, %s, %s, %s, %s, %s, %s, %s)".format('.'.join([self.schema, table]))
                    cursor = conn.cursor()
                    cursor.execute(query, (data[StationTable.station_id.value],
                                           data[StationTable.name.value],
                                           data[StationTable.type.value],
                                           data[StationTable.address.value],
                                           data[StationTable.latitude.value],
                                           data[StationTable.longitude.value],
                                           data[StationTable.altitude.value],
                                           data[StationTable.start_date.value]))
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

    def selectQuery(self, table_name, *columns, filter_table=None, info=' '):
        """Este método se encarga de realizar las consultas a todas las tablas de la base de datos del proyecto. Es lo
        suficientemente versatil como para entender varios tipos de consultas a las diferentes tablas"""
        query = []
        cursor = ''
        conn = ''
        str_query = ''
        if table_name == 'files':
            if columns[0] == 'file_name' and filter_table is None:
                str_query = "SELECT {0} FROM {1}".format(columns[0], '.'.join([self.schema, table_name]))

        # if table_name == 'buyorders' or table_name == 'sellorders' or table_name == 'openorders':
        #     if columns[0] == '*' and filter_table is None:
        #         str_query = 'SELECT * FROM {0}'.format(table_name)
        #
        #     elif columns[0] != '*' and filter_table is None:
        #         selected_columns = ', '.join(columns)
        #         str_query = 'SELECT ' + selected_columns + ' FROM {0}'.format(table_name)
        #
        #     elif columns[0] == '*' and filter_table is not None:
        #         str_query = "SELECT * FROM {0} WHERE id='{1}'".format(table_name, filter_table)
        #
        #     elif columns[0] != '*' and filter_table is not None:
        #         selected_columns = ', '.join(columns)
        #         str_query = "SELECT " + selected_columns + " FROM {0} WHERE id='{1}'".format(table_name, filter_table)
        #
        # elif table_name == 'capital':
        #     if (columns[0] == '*' or columns[0] == 'capital') and filter_table is None and table_name == 'capital':
        #         str_query = 'SELECT {0} FROM {1} ORDER BY id_capital DESC'.format(columns[0], table_name)
        #
        # elif table_name == 'users':
        #     if columns[0] == '*' and filter_table is not None:
        #         str_query = "SELECT {0} FROM {1} WHERE usuario='{2}'".format(columns[0], table_name, filter_table)

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
    config = BulidConfiguraion()
    dbController = DatabaseController(config)

    # script para llenar la tabla day
    # date_rng = list(pd.date_range(start='2016/01/01', end='2019/10/31', freq='D'))
    # tmp = []
    #
    #
    # for fila in date_rng:
    #     tmp.append(str(fila).split(' ')[0].split('-'))
    #
    # print(tmp)
    #
    # for dat in tmp:
    #     key = ''.join(dat)
    #     dat.insert(0, int(key))
    #     print(dat, '*****')
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











