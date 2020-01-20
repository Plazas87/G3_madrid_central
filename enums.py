from enum import Enum


class DBConfigEnum(Enum):
    """Contiene los campos necesarios para establecer conexión con la base de datos"""
    user = 0
    password = 1
    address = 2
    port = 3
    database = 4


class MainConfig(Enum):
    air_quality_path = 0
    air_stations_path = 1
    traffic_data_path = 2
    traffic_station_path = 3


class ChemicalComponents(Enum):
    """Pendiente hacer o definir el listdo de componentes que se van a estudiar"""