from kafka import KafkaProducer
import time


class TweetProducer:

    def __init__(self):
        pass

    def produce_from_txt():
        with open("__data__/cleansedData.txt", "r") as f:
            producer = KafkaProducer(bootstrap_servers='localhost:9092')
            x = 0
            for jsonLine in f:
                producer.send(
                  'tweet',
                  key=bytes([x]),
                  value=jsonLine.encode('utf-8')
                )
                x = x + 1