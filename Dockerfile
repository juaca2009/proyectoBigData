FROM gettyimages/spark

RUN apt update

RUN pip install numpy

RUN pip install networkx

RUN pip install pandas 

RUN pip install matplotlib 
