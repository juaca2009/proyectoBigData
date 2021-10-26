from kafka import KafkaProducer
from gestorArchivos import gestorArchivos
import time
import json

class Producer:
    def __init__(self, _topic, _freq, _ruta):
        self.__topic = _topic
        self.__freq = _freq if isinstance(_freq, int) else int(_freq)
        self.__producer = KafkaProducer(bootstrap_servers='localhost:9093',
                                        value_serializer=lambda x: json.dumps(x).encode('utf-8'))
        self.__gestor = gestorArchivos(_ruta)
        self.__datos = None

    def cargarDatos(self):
        self.__datos = self.__gestor.cargarArchivos()

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

if __name__ == '__main__':
    a = Producer('crimenes', 5, '../data/dataClean/')
    a.cargarDatos()
    a.enviarDatos()
