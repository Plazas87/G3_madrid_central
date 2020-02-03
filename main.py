import logging
import sys
from time_thread import Temporizador
from time import sleep
# import socket
#
# import win32serviceutil
# import servicemanager
# import win32event
# import win32service
#
# import time
# import random
# from pathlib import Path
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

checker = Temporizador('13:25:10', 15, mainController.start)
checker.start()

centinel = 0
while checker._estado:
    if centinel != 40:
        print('#################WAIT###########')
        sleep(2)
    else:
        checker.stop()

    centinel += 1



# class SMWinservice(win32serviceutil.ServiceFramework):
#     '''Base class to create winservice in Python'''
#
#     _svc_name_ = 'pythonService'
#     _svc_display_name_ = 'Python Service'
#     _svc_description_ = 'Python Service Description'
#
#     @classmethod
#     def parse_command_line(cls):
#         '''
#         ClassMethod to parse the command line
#         '''
#         print(cls)
#         win32serviceutil.HandleCommandLine(cls)
#
#     def __init__(self, args):
#         '''
#         Constructor of the winservice
#         '''
#         print('Inicializando')
#         win32serviceutil.ServiceFramework.__init__(self, args)
#         self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
#         socket.setdefaulttimeout(60)
#
#     def SvcStop(self):
#         '''
#         Called when the service is asked to stop
#         '''
#         self.stop()
#         self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
#         win32event.SetEvent(self.hWaitStop)
#
#     def SvcDoRun(self):
#         '''
#         Called when the service is asked to start
#         '''
#         self.start()
#         servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
#                               servicemanager.PYS_SERVICE_STARTED,
#                               (self._svc_name_, ''))
#
#         self.main()
#
#     def start(self):
#         '''
#         Override to add logic before the start
#         eg. running condition
#         '''
#         pass
#
#     def stop(self):
#         '''
#         Override to add logic before the stop
#         eg. invalidating running condition
#         '''
#         pass
#
#     def main(self):
#         '''
#         Main class to be ovverridden to add logic
#         '''
#         pass
#
#
# class PythonCornerExample(SMWinservice):
#     _svc_name_ = "PythonClimateTrafficReader"
#     _svc_display_name_ = "Python Data Reader"
#     _svc_description_ = "Read any file within repository"
#     # configurationObject = BulidConfiguraion()
#
#     def __init__(self):
#         pass
#
#     def start(self):
#         self.isrunning = True
#
#
#     def stop(self):
#         self.isrunning = False
#
#     def main(self):
#         # logging.basicConfig(level='DEBUG',
#         #                     filename='log.txt',
#         #                     filemode='a',
#         #                     format='%(asctime)s;%(levelname)s;%(name)s;%(module)s;%(funcName)s;%(message)s')
#
#         # self.mainController = Controller(self.ConfigurationObject)
#         while self.isrunning:
#             print(time.clock())
#             # self.conf.read_data()
#             # mainController.read_data()
#             time.sleep(3)
#
#
# if __name__ == '__main__':
#     PythonCornerExample.parse_command_line()
#     ConfigurationObject = BulidConfiguraion()
#     # mainController = Controller(ConfigurationObject)
#     # mainController.start()
#     # c = PythonCornerExample(mainController)
#     # PythonCornerExample.parse_command_line()


