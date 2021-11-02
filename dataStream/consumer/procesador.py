from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import *
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from consumer import consumer 

class procesador:
    def __init__(self, _topic):
        self.__spark = SparkSession.builder.appName("dataStream").getOrCreate()
        self.__spark.sparkContext.setLogLevel('WARN')
        self.__consumer = consumer(_topic)
        self.__schema = StructType() \
                       .add("Arrest", IntegerType()) \
                       .add("Beat", IntegerType()) \
                       .add("Block_index", DoubleType()) \
                       .add("Community Area", DoubleType()) \
                       .add("District", DoubleType()) \
                       .add("Domestic", IntegerType()) \
                       .add("FBI Code_index", DoubleType()) \
                       .add("IUCR_index", DoubleType()) \
                       .add("Location Description_index", DoubleType()) \
                       .add("X Coordinate", DoubleType()) \
                       .add("Y Coordinate", DoubleType()) \
                       .add("diaDel", IntegerType()) \
                       .add("horaDel", IntegerType()) \
                       .add("mesDel", IntegerType()) \
                       .add("minutoDel", IntegerType())
        self.__dataGeneral = self.__spark.createDataFrame(self.__spark.sparkContext.emptyRDD(), self.__schema)

    def convertirDataFrames(self, _data):
        return self.__spark.sparkContext.parallelize(_data).toDF() 

    def recibirMensajes(self):
        return self.__consumer.recibirMensajes()

    def agregarNuevaData(self, _newDataFrame):
        self.__dataGeneral = self.__dataGeneral.union(_newDataFrame)


    def getConsumer(self):
        return self.__consumer

    def getDataTemporal(self):
        return self.__consumer.getData()

    def getDataGeneral(self):
        return self.__dataGeneral

if __name__ == '__main__':
    a = procesador('crimenes')
    salida = True
    while salida:
        print("Esperando Datos")
        if a.recibirMensajes() == 1:
            print("Mensajes Recibido")
            dataTemp = a.getDataTemporal()
            dfTemporal = a.convertirDataFrames(dataTemp)
            a.agregarNuevaData(dfTemporal)
            b = a.getDataGeneral()
            print(b.show())
        else:
            print("No se ha recivo ningun mensaje")


