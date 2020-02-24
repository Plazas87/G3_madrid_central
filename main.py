import logging
import sys
import multiprocessing
from controller_one import Controller
from interface import Interface
from built_configuration import BuildConfiguration
import time

# Configuración del log de la aplicación
formatter = logging.basicConfig(level='DEBUG',
                                filename='log.txt',
                                filemode='a',
                                format='%(asctime)s;%(levelname)s;%(name)s;%(module)s;%(funcName)s;%(message)s')

rootLogger = logging.getLogger()
consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(formatter)
rootLogger.addHandler(consoleHandler)

if __name__ == '__main__':
    configurationObject = BuildConfiguration()
    mainController = Controller(configurationObject)
    mainController.start()

    contador = 0
    while True:
        time.sleep(20)
        if contador == 6:
            break
        mainController.read_data()
        contador += 1


# # Inicio de la aplicación
# ConfigurationObject = BulidConfiguraion()
# app = Interface(ConfigurationObject)
# app.start()
