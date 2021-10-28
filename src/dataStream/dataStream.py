from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

KAFKA_TOPIC_NAME = "crimenes"
KAFKA_BOOTSTRAP_SERVERS = 'kafka0:9093'

if __name__ == '__main__':
    spark=SparkSession.builder.appName('DataStream') \
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    crimenes_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka0:9093") \
        .option("subscribe", KAFKA_TOPIC_NAME) \
        .option("startingOffsets", "latest") \
        .load()

    crimenes_df1 = crimenes_df.selectExpr("CAST(value AS STRING)", "timestamp")

    crimenes_schema = StructType() \
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

    crimenes_total = crimenes_df1\
        .select(from_json(col("value"), crimenes_schema)\
        .alias("crimenes"), "timestamp")

    print(crimenes_total)

    crimenes_agg_write_stream = crimenes_total \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("Update") \
        .option("truncate", "false")\
        .format("console") \
        .start()
    crimenes_agg_write_stream.awaitTermination()
