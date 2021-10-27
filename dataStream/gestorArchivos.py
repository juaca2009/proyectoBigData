import pandas as pd
import glob

class gestorArchivos:
    def __init__(self, _ruta):
        self.__ruta = _ruta

    def cargarArchivos(self, _mes):
        archivos = glob.glob(self.__ruta + '*.csv')
        listaDataframes = list()
        for i in archivos:
            dataTemp = pd.read_csv(i, sep = ',',  header='infer')
            listaDataframes.append(dataTemp)
        dataTotal = pd.concat(listaDataframes, ignore_index=True)
        dataMes = dataTotal.loc[:, 'mesDel'] == _mes
        dataTotal = dataTotal.loc[dataMes]
        return dataTotal

    def getRuta(self):
        return self.__ruta

    def setRuta(self, _ruta):
        self.__ruta = _ruta



if __name__ == '__main__':
    a = gestorArchivos('../data/dataClean/')
    print(a.cargarArchivos(1))

