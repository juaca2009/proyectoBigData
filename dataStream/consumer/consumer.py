from kafka import KafkaConsumer
import json
import time

class consumer:
    def __init__(self, topic):
        self.__consumer = KafkaConsumer(topic,
                                       bootstrap_servers='localhost:9093',
                                       value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                                       group_id='nflx')
        self.__data = []

    def recibirMensajes(self):
        self.setData([])
        salida = self.getConsumer().poll(timeout_ms=120000)
        if salida != {}:
            for tp, messages in salida.items():
                for message in messages:
                    self.__data.append(message.value)
            return 1
        else:
            return 0

    def getConsumer(self):
        return self.__consumer

    def getData(self):
        return self.__data

    def setData(self, _data):
        self.__data = _data

#tiempo consumer 251 seg
#tiempo main 249 seg
if __name__ == '__main__':
    a = consumer('crimenes')
    salida = True
    while salida:
        print("Esperando Datos")
        if a.recibirMensajes() == 1:
            print("mensajes recividos")
        else:
            print("No se recivio mensajes")

