from kafka import KafkaProducer
from os import listdir


class StockProducer:

    def __init__(self, ):
        pass

    def produce_from_txt():
        files = listdir("__data__")
        for filename in files:
            if filename.endswith(".csv"):
                with open(filename, "r") as f:
                    producer = KafkaProducer(
                        bootstrap_servers='localhost:9092'
                    )
                    for csv in f:
                        producer.send(
                          'stock',
                          csv
                        )