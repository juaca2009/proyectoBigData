import sys
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline

#definicion funciones
def eliminarColumnas(_data, _Lcolumnas):
    return _data.select([column for column in _data.columns if column not in _Lcolumnas]) 

def eliminarColumnasNulos(_data, _Lcolumnas):
    return _data.dropna(how='any', subset=_Lcolumnas)

def serializarColumnas(_data, _Lcolumnas):
    indexers = [StringIndexer(inputCol=column, outputCol=column+"_index").fit(df) for column in _Lcolumnas]
    pipeline = Pipeline(stages=indexers)
    dfTemp = pipeline.fit(df).transform(df)
    return dfTemp


if __name__ == "__main__":
    #inicio sesion spark
    spark=SparkSession.builder.appName('DataCleaning').getOrCreate()

    #carga de datos 
    df = spark.read.csv(sys.argv[1], sep=',', header= True, inferSchema=True)

    #columnas a eliminar
    columnasEliminar = ['_c0', 'ID', 'Case Number', 'Primary Type', 'Description', 'Ward', 'Year', 'Latitude', 'Longitude', 'Location']
    dataColumnas = eliminarColumnas(df, columnasEliminar)     

    #elimanar filas con valores nulos 
    columnasNulos = ['District', 'Community Area', 'X Coordinate', 'Y Coordinate', 'Location Description']
    dataSNula = eliminarColumnasNulos(dataColumnas, columnasNulos)

    #serializar columnas
    columnasSerializar = ['IUCR', 'Location Description', 'FBI Code']
    dataTemp = serializarColumnas(dataSNula, columnasSerializar) 
    dataSerializada = eliminarColumnas(dataTemp, columnasSerializar) #eliminar columnas no serializadas
