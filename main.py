import logging
import sys
from time_thread import UpdateChecker
from time import sleep

from built_configuration import BulidConfiguraion
from controller import Controller

# Configuración del log de la aplicación
formatter = logging.basicConfig(level='DEBUG',
                                filename='log.txt',
                                filemode='a',
                                format='%(asctime)s;%(levelname)s;%(name)s;%(module)s;%(funcName)s;%(message)s')

rootLogger = logging.getLogger()
consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(formatter)
rootLogger.addHandler(consoleHandler)

# Inicio de la aplicación
ConfigurationObject = BulidConfiguraion()
mainController = Controller(ConfigurationObject)
mainController.start()


checker = UpdateChecker(15, mainController.read_data)
checker.start()

centinel = 0

while checker._status:
    if centinel != 40:
        print('****Main process****** each second print this line')
        sleep(2)
    else:
        checker.stop()

    centinel += 1
