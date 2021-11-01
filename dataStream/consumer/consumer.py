from kafka import KafkaConsumer
import json

class consumer:
    def __init__(self, topic):
        self.__consumer = KafkaConsumer(topic,
                                       bootstrap_servers='localhost:9093',
                                       value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                                       group_id='nflx')
        self.__data = []

    def recibirMensajes(self):
        contador = 0
        for i in self.getConsumer():
            i = i.value
            print(f'Message {contador}: {i}')
            self.__data.append(i)
            contador = contador + 1


    def getConsumer(self):
        return self.__consumer


if __name__ == '__main__':
    a = consumer('crimenes')
    a.recibirMensajes()
