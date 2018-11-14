from kafka import KafkaProducer
from os import listdir


class StockProducer:

    def __init__(self, ):
        pass

    def produce_from_txt():
        with open("__data__/tweet_sentiment.txt", "r") as f:
            producer = KafkaProducer(
                bootstrap_servers='localhost:9092'
            )
            for csv in f:
                producer.send(
                  'tweet_sa',
                  csv
                )