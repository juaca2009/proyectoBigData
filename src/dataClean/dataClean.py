import sys
from pyspark.sql import SparkSession

#definicion funciones
def eliminarColumnas(_data, _Lcolumnas):
    return _data.select([column for column in _data.columns if column not in _Lcolumnas]) 

def eliminarColumnasNulos(_data, _Lcolumnas):
    return _data.dropna(how='any', subset=_Lcolumnas)


if __name__ == "__main__":
    #inicio sesion spark
    spark=SparkSession.builder.appName('DataCleaning').getOrCreate()

    #carga de datos 
    df = spark.read.csv(sys.argv[1], sep=',', header= True, inferSchema=True)

    #columnas a eliminar
    columnasEliminar = ['_c0', 'ID', 'Case Number', 'Primary Type', 'Description', 'Ward', 'Year', 'Latitude', 'Longitude', 'Location']
    dataColumnas = eliminarColumnas(df, columnasEliminar)     

    #elimanar filas con valores nulos 
    columnasNulos = ['Case Number', 'District', 'Community Area', 'X Coordinate', 'Y Coordinate', 'Location Description']
    dataSNula = eliminarColumnasNulos(dataColumnas, columnasNulos)
    
