import sys
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import *


if __name__ == "__main__":
    #inicio sesion spark
    spark=SparkSession.builder.appName('MachineLearning').getOrCreate()

    #carga de datos
    df = spark.read.csv("/tmp/data/dataClean/*.csv", sep=',', header= True, inferSchema=True)

    #vectorizacion de los atributos
    vector = VectorAssembler(inputCols = ['Domestic', 'Beat', 'District', 'Community Area', 'X Coordinate', 'Y Coordinate', 
                                          'IUCR_index', 'Location Description_index', 'FBI Code_index', 'Block_index', 
                                          'mesDel', 'diaDel', 'horaDel', 'minutoDel'], outputCol = 'atributos')
    df = vector.transform(df)
    df = df.select('atributos', 'Arrest')
