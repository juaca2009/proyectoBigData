import sys
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import *
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator


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
    df = df.selectExpr("atributos as atributos", "Arrest as label")

    #division del dataset 70% entrenamiento - 30% pruebas
    train, test = df.randomSplit([0.7, 0.3], seed = 2018)

    #instacia del evaluador
    evaluator = BinaryClassificationEvaluator()

    #regresion logistica
    lr = LogisticRegression(featuresCol = 'atributos', labelCol = 'label', maxIter=10)
    lrModel = lr.fit(train)
    predictions = lrModel.transform(test)
    print('Test Area Under ROC', evaluator.evaluate(predictions))


    #arboles de decision
    dt = DecisionTreeClassifier(featuresCol = 'atributos', labelCol = 'label', maxDepth = 3)
    dtModel = dt.fit(train)
    predictionsDt = dtModel.transform(test)
    print("Test Area Under ROC: " + str(evaluator.evaluate(predictionsDt, {evaluator.metricName: "areaUnderROC"})))

    #random forest 
    rf = RandomForestClassifier(featuresCol = 'atributos', labelCol = 'label')
    rfModel = rf.fit(train)
    predictionsRf = rfModel.transform(test)
    print("Test Area Under ROC: " + str(evaluator.evaluate(predictionsRf, {evaluator.metricName: "areaUnderROC"})))
