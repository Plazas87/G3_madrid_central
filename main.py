import pandas as pd
from built_configuration import BulidConfiguraion
from climate_file_reader import ClimateFileReader
from controller import Controller
import logging


logging.basicConfig(level='DEBUG',
                        filename='log.txt',
                        filemode='a',
                        format='%(asctime)s;%(levelname)s;%(name)s;%(module)s;%(funcName)s;%(message)s')

ConfigurationObject = BulidConfiguraion()
mainController = Controller(ConfigurationObject)
mainController.read_data()

