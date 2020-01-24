from enum import Enum


class DBConfigEnum(Enum):
    """Contiene los campos necesarios para establecer conexión con la base de datos"""
    user = 0
    password = 1
    address = 2
    port = 3
    database = 4
    schema = 5


class DBTable(Enum):
    measurement = 0
    station = 1
    time = 2
    day = 3
    magnitude = 4


class MainConfig(Enum):
    air_quality_path = 0
    air_stations_path = 1
    traffic_data_path = 2
    traffic_station_path = 3


class MeasurementTable(Enum):
    station_id = 0
    day_id = 1
    time_id = 2
    magnitude_id = 3
    value = 4
    validation = 5


class StationTable(Enum):
    station_id = 0
    name = 1
    type = 2
    address = 3
    latitude = 4
    longitude = 5
    altitude = 6
    start_date = 7


class TimeTable(Enum):
    time_id = 0
    hour = 1


class DayTable(Enum):
    day_id = 0
    day = 1
    month = 2
    year = 3


class MagnitudeTable(Enum):
    magnitude_id = 0
    name = 1
    abbreviation = 2
    max_value_excelent = 4
    min_value_good = 5
    max_value_good = 6
    min_value_acceptable = 6
    max_value_acceptable = 7
    min_value_bad = 8


class ChemicalComponents(Enum):
    """Pendiente hacer o definir el listdo de componentes que se van a estudiar"""