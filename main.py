import logging
import sys
from interface import Interface
from built_configuration import BulidConfiguraion

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
app = Interface(ConfigurationObject)
app.start()
