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
        self.__evaluador = BinaryClassificationEvaluator()
        self.lr = LogisticRegression(featuresCol = 'atributos', labelCol = 'label', maxIter=10)
        self.dt = DecisionTreeClassifier(featuresCol = 'atributos', labelCol = 'label', maxDepth = 3)
        self.rf = RandomForestClassifier(featuresCol = 'atributos', labelCol = 'label')

    def convertirDataFrames(self, _data):
        return self.__spark.sparkContext.parallelize(_data).toDF() 

    def recibirMensajes(self):
        return self.__consumer.recibirMensajes()

    def agregarNuevaData(self, _newDataFrame):
        self.__dataGeneral = self.__dataGeneral.union(_newDataFrame)

    def prepararData(self):
        vector = VectorAssembler(inputCols = ['Domestic', 'Beat', 'District', 'Community Area', 'X Coordinate', 'Y Coordinate', 
                                          'IUCR_index', 'Location Description_index', 'FBI Code_index', 'Block_index', 
                                          'mesDel', 'diaDel', 'horaDel', 'minutoDel'], outputCol = 'atributos')
        dfVectorizado = vector.transform(self.__dataGeneral)
        dfVectorizado = dfVectorizado.select('atributos', 'Arrest')
        dfVectorizado = dfVectorizado.selectExpr("atributos as atributos", "Arrest as label")
        return dfVectorizado

    def probarRegresionLogistica(self, _train, _test):
        lrModel = self.lr.fit(_train)
        predictions = lrModel.transform(_test)
        accuracy = predictions.filter(predictions.label == predictions.prediction).count() / float(predictions.count())
        return [self.__evaluador.evaluate(predictions), accuracy]

    def proabarArbolesDecision(self, _train, _test):
        dtModel = self.dt.fit(_train)
        predictionsDt = dtModel.transform(_test)
        accuracy2 = predictionsDt.filter(predictionsDt.label == predictionsDt.prediction).count() / float(predictionsDt.count())
        return [self.__evaluador.evaluate(predictionsDt), accuracy2]

    def probarRandomForest(self, _train, _test):
        rfModel = self.rf.fit(_train)
        predictionsRf = rfModel.transform(_test)
        accuracy3 = predictionsRf.filter(predictionsRf.label == predictionsRf.prediction).count() / float(predictionsRf.count())
        return [self.__evaluador.evaluate(predictionsRf), accuracy3]

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
            print("Mensajes Recibidos")
            dataTemp = a.getDataTemporal()
            dfTemporal = a.convertirDataFrames(dataTemp)
            a.agregarNuevaData(dfTemporal)
            dfV = a.prepararData()
            train, test = dfV.randomSplit([0.7, 0.3], seed = 2018)
            print("##########################################################################################")
            print("cantidad de datos para modelos: ", a.getDataGeneral().count())
            print("===========================================================================================")
            print("Regresion Logistica")
            regresion = a.probarRegresionLogistica(train, test)
            print("Area bajo el ROC: ", regresion[0])
            print("Precision: ", regresion[1])
            print("===========================================================================================")
            print("arboles de decision:")
            decision = a.proabarArbolesDecision(train, test)
            print("Area bajo el ROC: ", decision[0])
            print("Precision: ", decision[1])
            print("===========================================================================================")
            print("Random Forest")
            random = a.probarRandomForest(train, test)
            print("Area bajo el ROC: ", random[0])
            print("Precision: ", random[1])
            print("##########################################################################################")
        else:
            print("No se ha recivo ningun mensaje")


