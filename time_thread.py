#!/usr/bin/env python
# -*- coding: latin-1 -*-

from datetime import datetime, timedelta
from threading import Thread
from time import sleep


class Temporizador(Thread):
    def __init__(self, hora, delay, funcion):
        # El constructor recibe como parámetros:
        ## hora = en un string con formato hh:mm:ss y es la hora a la que queremos que se ejecute la función.
        ## delay = tiempo de espera entre comprobaciones en segundos.
        ## funcion = función a ejecutar.

        super(Temporizador, self).__init__()
        self._estado = True
        self.hora = hora
        self.delay = delay
        self.funcion = funcion

    def stop(self):
        self._estado = False

    def run(self):
        # # Pasamos el string a dato tipo datetime
        # hora_aux = datetime.strptime(self.hora, '%H:%M:%S')
        # Obtenemos la fecha y hora actuales.
        hora = datetime.now()
        # Comprobamos si la hora ya a pasado o no, si ha pasado sumamos una hora (hoy ya no se ejecutará).

        print('Ejecución automática iniciada : Desde hilo')
        # print('Proxima ejecución programada el {0} a las {1}'.format(hora.date(), hora.time()))

        # Iniciamos el ciclo:
        while self._estado:
            # Comparamos la hora actual con la de ejecución y ejecutamos o no la función.
            self.funcion()
            print('Ejecución programada ejecutada el {0} a las {1}'.format(hora.date(), hora.time()))
            nx = hora + timedelta(seconds=self.delay)
            print('Próxima ejecución programada el {0} a las {1}'.format(hora.date(), nx))

            # Esperamos x segundos para volver a ejecutar la comprobación.
            sleep(self.delay)

        # Si usamos el método stop() salimos del ciclo y el hilo terminará.
        else:
            print('Ejecución automática finalizada')


if __name__ == '__main__':

    def camilo():
        print('***********************HILO************************************')


    tempo = Temporizador('13:25:10', 15, camilo)
    tempo.start()

    for i in range(0, 10):
        print('Printed from main threat')
        sleep(5)

    tempo.stop()


