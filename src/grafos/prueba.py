from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from graphframes import *

spark=SparkSession.builder.appName('pruebasGrafos').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
v = spark.createDataFrame([
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 30),
], ["id", "name", "age"])
# Create an Edge DataFrame with "src" and "dst" columns
e = spark.createDataFrame([
  ("a", "b", "friend"),
  ("b", "c", "follow"),
  ("c", "b", "follow"),
], ["src", "dst", "relationship"])

g = GraphFrame(v, e)

g.inDegrees.show()

g.edges.filter("relationship = 'follow'").count()

results = g.pageRank(resetProbability=0.01, maxIter=20)
results.vertices.select("id", "pagerank").show()

