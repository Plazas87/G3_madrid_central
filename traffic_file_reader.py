#! usr/bin/env python3

import pandas as pd
import file_reader as fr


class TrafficFileReader(fr.FileReader):
    """Esta clase contiene las funciones y la información necesaria para realizarlectura de los archivos .csv que
     esxitan dentro de la carpeta resources realcionados con el clima"""
    def __init__(self, rutaArchivo, extencion):
        fr.FileReader.__init__(self, rutaArchivo, extencion)

    # def load_files(self):
    #     """Esta función se encarga de leer todos los archivos en el directorio que cumplen
    #     con el criterio de la extención"""
    #     stream = open(self.pathName + self.files[0])
    #     data = []
    #     while True:
    #         data.append(stream.readline())
    #         if not data:
    #             break
    #     stream.close()
    #     print(data)


if __name__ == '__main__':
    c = TrafficFileReader()
    c.load_files()
    print(c)
