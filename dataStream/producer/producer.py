from kafka import KafkaProducer
from gestorArchivos import gestorArchivos
import time
import json

class Producer:
    def __init__(self, _topic, _freq, _ruta):
        self.__topic = _topic
        self.__freq = _freq if isinstance(_freq, int) else int(_freq)
        self.__producer = KafkaProducer(bootstrap_servers='localhost:9093',
                                        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                                        batch_size = 1638400, buffer_memory = 33554432, linger_ms= 60000)
        self.__gestor = gestorArchivos(_ruta)
        self.__datos = None

    def cargarDatos(self, _mes):
        self.__datos = self.__gestor.cargarArchivos(_mes)

    def cambiarDatos(self, _ruta, _mes):
        self.__gestor.setRuta(_ruta)
        self.cargarDatos(_mes)

    def enviarDatos(self):
        for index, valor in self.__datos.iterrows():
            dict_data = dict(valor)
            self.__producer.send(self.__topic, value=dict_data)
            print(f'Message {index + 1}: {dict_data}')
            time.sleep(self.__freq)
    
    def getProducer(self):
        return self.__producer
    
    def getDatos(self):
        return self.__datos

    def getTopic(self):
        return self.__topic

    def getFreq(self):
        return self.__freq

    def setFreq(self, _freq):
        self.__freq = _freq

    def setTopic(self, _topic):
        self.__topic = _topic

if __name__ == '__main__':
    a = Producer('crimenes', 1, '../../data/dataClean/')
    a.cargarDatos(1)
    a.enviarDatos()
