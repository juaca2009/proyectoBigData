from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import *
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from consumer import consumer 

class procesador:
    def __init__(self, _topic):
        self.__schema = StructType() \
                       .add("Arrest", IntegerType()) \
                       .add("Domestic", IntegerType()) \
                       .add("Beat", IntegerType()) \
                       .add("District", DoubleType()) \
                       .add("Community Area", DoubleType()) \
                       .add("X Coordinate", DoubleType()) \
                       .add("Y Coordinate", DoubleType()) \
                       .add("IUCR_index", DoubleType()) \
                       .add("Location Description_index", DoubleType()) \
                       .add("FBI Code_index", DoubleType()) \
                       .add("Block_index", DoubleType()) \
                       .add("mesDel", IntegerType()) \
                       .add("diaDel", IntegerType()) \
                       .add("horaDel", IntegerType()) \
                       .add("minutoDel", IntegerType())

        self.__consumer = consumer(_topic)

    def getSchema(self):
        return self.__schema

    def getConsumer(self):
        return self.__consumer

