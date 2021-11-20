# proyectoBigData
Proyecto del curso de Big Data, utilizando servicio dockerizado de apache spark, servicio que contiene
un container master y dos containers worker. El master escucha el en puerto 9080, el worker1 y worker2 escuchan en los puertos 
8081 y 8082 correspondientemente. 

## Configuracion DataSet 
Los datos que se utilizan para este proyecto se encuentran en el siguiente enlace: https://www.kaggle.com/currie32/crimes-in-chicago?select=Chicago_Crimes_2012_to_2017.csv.
Especificamente se utilizan los datos comprendidos entre el año 2012 y el año 2017 (367 MB).   
Al ser un archivo demasiado grande no se carga dentro del repositorio del proyecto. Para agregar los datos al proyecto, se deben descargar de la pagina y posteriormente, crear una carpeta con el nombre "data" dentro del directorio de proyecto. 
Finalmente se debe copiar el dataset descargado dentro de esta carpeta. 

Para la realizacion del grafo es necesario agregar dos datasets nuevos: https://www.kaggle.com/chicago/chicago-police-stations/version/4, https://www.kaggle.com/chicago/chicago-public-schools-data?select=chicago-public-schools-high-school-progress-report-card-2012-2013.csv.
Estos archivos se deben descargar y descomprimir dentro de la carpeta data, nombrada anteriormente.

## Configuracion entorno
Para configuirar el entorno primero se debe crear una imgen docker con el dockerFile.
```
sudo docker build -t "spark" .
```
Posteriormente se debe correr el archivo docker compose.
```
sudo docker-compose up
```

## Configuiracion para Correr script del grafo
Para poder crear y manipular el grafo es necesario instalar las librerias de graphFrames por medio de la bandera package:
```
spark-2.4.1/bin/spark-submit --master spark://master:7077 --packages graphframes:graphframes:0.8.1-spark2.4-s_2.11 --repositories https://repos.spark-packages.org /usr/src/grafos/analisisGrafo.py
```
