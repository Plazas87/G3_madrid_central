#! usr/bin/env

from configparser import ConfigParser
import logging
import enums as en


class BulidConfiguraion:
    """Esta clase se encarga de leer el archivo de configuración genral, las parametros leidos son almacenados como
    propiedades del objeto que sera luego instanciado en la clase controller"""
    def __init__(self):
        logging.info("Built main configuration object")
        param = self.__config()
        self.user = param['postgresql'][en.DBConfigEnum.user.name]
        self.password = param['postgresql'][en.DBConfigEnum.password.name]
        self.address = param['postgresql'][en.DBConfigEnum.address.name]
        self.port = param['postgresql'][en.DBConfigEnum.port.name]
        self.database = param['postgresql'][en.DBConfigEnum.database.name]
        self.schema = param['postgresql'][en.DBConfigEnum.schema.name]

        # Built configuration for dataframes
        self.air_quality_path = param['paths'][en.MainConfig.air_quality_path.name]
        self.air_stations_path = param['paths'][en.MainConfig.air_stations_path.name]
        self.traffic_data_path = param['paths'][en.MainConfig.traffic_data_path.name]
        self.traffic_station_path = param['paths'][en.MainConfig.traffic_station_path.name]

    def __config(self, section='postgresql', filename='configpostgres.ini'):
        """Configura los parámetros para la conexión con la base de datos a través de la lectura de un
        archivo de configuración de extención .ini"""
        conf = ConfigParser()
        try:
            conf.read(filename)
            config_file_dict = {}
            tmp = {}
            for sect in conf.sections():
                params = conf.items(sect)
                for param in params:
                    tmp[param[0]] = param[1]

                config_file_dict[sect] = tmp
                tmp = {}

            return config_file_dict

        except Exception as e:
            print(e)

    def __str__(self):
        return 'user:' + ' ' + self.user + '\npassword:' + ' ' + self.password + '\naddress:' + ' ' + self.address + \
               '\nport:' + ' ' + self.port + '\ndatabase:' + ' ' + self.database + \
               '\nair_quality_path:' + ' ' + self.air_quality_path + '\nair_stations_path:' + ' ' + \
               self.air_stations_path + '\ntraffic_data_path:' + ' ' + self.traffic_data_path + \
               '\ntraffic_station_path:' + ' ' + self.traffic_station_path


if __name__ == '__main__':
    c = BulidConfiguraion()
    print(c)


