from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from graphframes import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib


def imprimirGrafo(_listaC, _listaN):
    Gplot=nx.Graph()
    nod = _listaN.toPandas()
    for row in _listaC.select('src','dst').where((_listaC.src == 1) | (_listaC.src == 2) | (_listaC.src == 3) | (_listaC.src == 4) | (_listaC.src == 77)).collect():
        Gplot.add_edge(row['src'],row['dst'])
    plt.subplot(121)
    nod = nod.set_index('id')
    nod = nod.reindex(Gplot.nodes())
    nod['tipoNodo'] = pd.Categorical(nod['tipoNodo']) 
    nod['tipoNodo'].cat.codes
    cmap = matplotlib.colors.ListedColormap(['C0', 'darkorange', 'green']) #naranja colegios, verdes estaciones
    node_sizes = [50 if entry == 'Estacion Policia' or entry == 'Colegio' else 100 for entry in nod.tipoNodo]
    nx.draw(Gplot, with_labels=True, font_size=1, arrows=True, node_color=nod['tipoNodo'].cat.codes, cmap=cmap, node_size=node_sizes, width=1)
    plt.savefig('/tmp/data/cerca.png', dpi=600)
    plt.show()



spark=SparkSession.builder.appName('pruebasGrafos').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

#carga de datos
estacionesDf = spark.read.csv("/tmp/data/police-stations.csv", sep=',', header= True, inferSchema=True)
colegiosDf = spark.read.csv("/tmp/data/chicago-public-schools-elementary-school.csv", sep=',', header=True, inferSchema=True)
crimenesDf = spark.read.csv("/tmp/data/dataClean/*.csv", sep=',', header= True, inferSchema=True)
colindasDf = spark.read.csv("/tmp/data/dataset_colindantes.csv", sep=';', header=True, inferSchema=True)

#Creacion dataFrames nodos y conexiones 
schemaNodos = StructType() \
              .add("id", IntegerType()) \
              .add("tipoNodo", StringType())

schemaConexiones = StructType()\
                   .add("src", IntegerType()) \
                   .add("dst", IntegerType()) \
                   .add("relacion", StringType())

nodos = spark.createDataFrame(spark.sparkContext.emptyRDD(), schemaNodos)
conexiones = spark.createDataFrame(spark.sparkContext.emptyRDD(), schemaConexiones)

#agregar nodos tipo area comunitaria
aComunitarias = crimenesDf.select('Community Area').distinct()
aComunitarias = aComunitarias.withColumn("Community Area", aComunitarias["Community Area"].cast(IntegerType()))
aComunitarias = aComunitarias.withColumn("tipoNodo", lit("Area Comunitaria"))
nodos = nodos.union(aComunitarias)

#agregar nodos estaciones de policia 
estacionesDf = estacionesDf.dropDuplicates(["ZIP"])
estacionesDf = estacionesDf.withColumn("id", estacionesDf["ZIP"]) #generar columna id para cada estacion 
estaciones = estacionesDf.select("id").distinct()
estaciones = estaciones.withColumn("tipoNodo", lit("Estacion Policia"))
nodos = nodos.union(estaciones)

#agregar nodos tipo colegios
colegios = colegiosDf.select("School ID").distinct()
colegios = colegios.withColumn("tipoNodo", lit("Colegio"))
nodos = nodos.union(colegios)

#agregar conexiones areaC-colegio
ac = colegiosDf.select("Community Areas", "School ID")
ac = ac.withColumn("relacion", lit("Posee Colegio"))
conexiones = conexiones.union(ac)

#agregar conexiones areas colindantes
aa = colindasDf.withColumn("relacion", lit("Colinda"))
conexiones = conexiones.union(aa)

#agregar conexiones areaC-estaciones
ae = estacionesDf.select("Community Areas", "id")
ae = ae.withColumn("relacion", lit("Posee Estacion"))
conexiones = conexiones.union(ae)
conexiones = conexiones.dropna(how='any')

g = GraphFrame(nodos, conexiones)
imprimirGrafo(conexiones, nodos)

#g.inDegrees.show()

#g.edges.filter("relationship = 'follow'").count()

#results = g.pageRank(resetProbability=0.01, maxIter=20)
#results.vertices.select("id", "pagerank").show()

