import sqlite3
import psycopg2 as db
import logging
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

    def __new__(cls, name=None, params=None):
        if not hasattr(cls, 'instance'):  # Si no existe el atributo “instance”
            cls.instance = super(DatabaseController, cls).__new__(cls)  # lo creamos
        return cls.instance

    # Este código se encuentra comentado por que era el que se usaba para la conexión con sqlite3
    # def connect(self):
    #     try:
    #         conn = db.connect('Bitacora.db')
    #         return conn
    #     except Exception as e:
    #         print(e, '- Error in model.py: {} method connect'.format(e.__traceback__.tb_lineno))

    def connect(self):
        logging.info('Connecting with database')
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

    def insert(self, table, data):
        # Crea la conexión con la base de datos
        conn = self.connect()
        if conn is not None:
            if table == 'files':
                try:
                    # = "INSERT INTO {0} (file_name) VALUES (%s)".format(table)
                    # print(query, value)
                    print(data)
                    cursor = conn.cursor()
                    for value in data:
                        cursor.execute("INSERT INTO files (file_name) VALUES (%s)", (value,))
                        print('La orden ha sido almacenada correctamente en la base de datos')
                     cursor.comm

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

        #     if order[OpenOrderElement.OrderType.name] == OrderTypes.Compra.name:
        #         try:
        #             with conn:
        #                 cursor = conn.cursor()
        #                 cursor.execute(
        #                     "INSERT INTO buyOrders(timestamp, id, ticker, ordertype, buyprice, cantidad) VALUES (%s,%s,%s,%s,%s,%s)",
        #                     (order[OpenOrderElement.TimeStamp.name], order[OpenOrderElement.Id.name], order[OpenOrderElement.Ticker.name],
        #                      order[OpenOrderElement.OrderType.name], order[OpenOrderElement.BuyPrice.name],
        #                      order[OpenOrderElement.OpenCantidad.name]))
        #                 # print('    La orden ha sido almacenada correctamente en la base de datos')
        #         except sqlite3.IntegrityError as e:
        #             print(e, ' - No se puede agregar la orden a la BD')
        #
        #         try:
        #             with conn:
        #                 cursor = conn.cursor()
        #                 cursor.execute(
        #                     "INSERT INTO openorders(timestamp, id, ticker, ordertype, buyprice, cantidad) VALUES (%s,%s,%s,%s,%s,%s)",
        #                     (order[OpenOrderElement.TimeStamp.name], order[OpenOrderElement.Id.name], order[OpenOrderElement.Ticker.name],
        #                      order[OpenOrderElement.OrderType.name], order[OpenOrderElement.BuyPrice.name],
        #                      order[OpenOrderElement.OpenCantidad.name]))
        #                 # print('    La orden ha sido almacenada correctamente en la base de datos')
        #         except sqlite3.IntegrityError as e:
        #             print(e, ' - No se puede agregar la orden a la BD')
        #
        #     elif order[OpenOrderElement.OrderType.name] == OrderTypes.Venta.name:
        #         try:
        #             with conn:
        #                 cursor = conn.cursor()
        #                 cursor.execute(
        #                     "INSERT INTO sellorders(timestamp, id, ticker, ordertype, sellprice, cantidad) VALUES (%s,%s,%s,%s,%s,%s)",
        #                     (order[CloseOrderElement.TimeStamp.name], order[CloseOrderElement.Id.name], order[CloseOrderElement.Ticker.name],
        #                      order[CloseOrderElement.OrderType.name], order[CloseOrderElement.SellPrice.name],
        #                      order[CloseOrderElement.CloseCantidad.name]))
        #                 # print('    La orden de venta ha sido almacenada correctamente en la base de datos ')
        #         except sqlite3.IntegrityError as e:
        #             print('No se puede agregar la orden a la BD')
        #
        #     conn.close()
        # else:
        #     print('No se puede establecer una comunicación con la base de datos')

    def update_capital(self, time_stamp, value):
        try:
            conn = self.connect()
            with conn:
                cursor = conn.cursor()
                cursor.execute("INSERT INTO capital (timestamp, capital) VALUES (%s,%s)", (time_stamp, value))
        except sqlite3.IntegrityError as e:
            print(e, '- Error in model.py: {} method update_capital'.format(e.__traceback__.tb_lineno))

    def update_open_orders(self, id_, value=0, operation='update'):
        """Esta función se encarga de actualizar los valores de la cantidad de acciones en el portadolio.
        Esta función debe llamarse siempre que se ejecute una orden de venta"""
        if operation == 'update':
            try:
                conn = self.connect()
                strquery = 'UPDATE openOrders SET cantidad= %s WHERE id= %s'
                values = (value, id_)
                with conn:
                    cursor = conn.cursor()
                    cursor.execute(strquery, values)
            except sqlite3.IntegrityError as e:
                print(e)
                print('No se puede modificar el capital en la base de datos')

        elif operation == 'delete':
            try:
                conn = self.connect()
                strquery = 'DELETE FROM openOrders WHERE id= %s'
                values = (id_,)
                with conn:
                    cursor = conn.cursor()
                    cursor.execute(strquery, values)
                    # print('se ha elimiado la orden {} que estaba abierta'.format(id_))
            except sqlite3.IntegrityError as e:
                print(e)
                print('No se puede modificar el capital en la base de datos')

    def selectQuery(self, table_name, *columns, filter_table=None):
        """Este método se encarga de realizar las consultas a todas las tablas de la base de datos del proyecto. Es lo
        suficientemente versatil como para entender varios tipos de consultas a las diferentes tablas"""
        query = []
        cursor = ''
        conn = ''
        str_query = ''
        if table_name == 'buyorders' or table_name == 'sellorders' or table_name == 'openorders':
            if columns[0] == '*' and filter_table is None:
                str_query = 'SELECT * FROM {0}'.format(table_name)

            elif columns[0] != '*' and filter_table is None:
                selected_columns = ', '.join(columns)
                str_query = 'SELECT ' + selected_columns + ' FROM {0}'.format(table_name)

            elif columns[0] == '*' and filter_table is not None:
                str_query = "SELECT * FROM {0} WHERE id='{1}'".format(table_name, filter_table)

            elif columns[0] != '*' and filter_table is not None:
                selected_columns = ', '.join(columns)
                str_query = "SELECT " + selected_columns + " FROM {0} WHERE id='{1}'".format(table_name, filter_table)

        elif table_name == 'capital':
            if (columns[0] == '*' or columns[0] == 'capital') and filter_table is None and table_name == 'capital':
                str_query = 'SELECT {0} FROM {1} ORDER BY id_capital DESC'.format(columns[0], table_name)

        elif table_name == 'users':
            if columns[0] == '*' and filter_table is not None:
                str_query = "SELECT {0} FROM {1} WHERE usuario='{2}'".format(columns[0], table_name, filter_table)

        if str_query != '':
            try:
                conn = self.connect()
                if conn is not None:
                    cursor = conn.cursor()
                    cursor.execute(str_query)
                    query = cursor.fetchall()
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

        else:
            return None

    def __query_to_dict(self, query):
        """Esta función se encarga de transformar los objetos producidos por la clase order en diccionarios. Esta función
        es opcional; su uso node siempre se requiere"""
        lst = []
        for row in query:
            dict_order = {OpenOrderElement.TimeStamp.name: row[OpenOrderElement.TimeStamp.value],
                          OpenOrderElement.Id.name: row[OpenOrderElement.Id.value],
                          OpenOrderElement.Ticker.name: row[OpenOrderElement.Ticker.value],
                          OpenOrderElement.OrderType.name: row[OpenOrderElement.OrderType.value],
                          OpenOrderElement.BuyPrice.name: row[OpenOrderElement.BuyPrice.value],
                          OpenOrderElement.OpenCantidad.name: row[OpenOrderElement.OpenCantidad.value]}
            lst.append(dict_order)
        return lst

    def load_open_orders(self):
        """Este método se encarga de poner en memoria las ordenes abiertas en el portafokio de modo que el usuario pueda
        verlas en la pantalla cada vez que vaya a abrir o cerrar una orden"""
        conn = self.connect()
        strquery = 'SELECT * FROM openorders'
        with conn:
            cur = conn.cursor()
            try:
                cur.execute(strquery)
                query = cur.fetchall()
                dict_query = self.__query_to_dict(query)
                return dict_query
            except Exception as e:
                print(e, '- Error in model.py: {} method load_open_orders'.format(e.__traceback__.tb_lineno))
                return None

    def __config(self, filename='configpostgres.ini', section='postgresql'):
        # create a parser
        """Configura los parámetros para la conexión con la base de datos a través de la lectura de un
        archivo de configuración de extención .ini"""
        conf = ConfigParser()
        try:
            conf.read(filename)
            db_query = {}
            if conf.has_section(section):
                params = conf.items(section)
                for param in params:
                    db_query[param[0]] = param[1]

            return db_query

        except Exception as e:
            print(e, 'Section {0} not found in the {1} file'.format(section, filename))


if __name__ == '__main__':
    query = DatabaseController()
    var = query.selectQuery('users', '*', filter_table='acpr87@gmail.com')

