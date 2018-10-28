from cassandra.cluster import Cluster
from kafka import KafkaConsumer
import json

cluster = Cluster()
session = cluster.connect('twittersa')
consumer = KafkaConsumer('tweetSA', auto_offset_reset='earliest',
                         bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)

for msg in consumer:
    d = json.loads(msg.value)
    session.execute("INSERT INTO tweets (created_at, followers_count, text, timestamp_ms, sentiment, company) "
                    "VALUES (%s, %s, %s, %s, %s, %s);", (d['created_at'], d['followers_count'],
                                                         d['text'], d['timestamp_ms'],
                                                         d['sentiment'], d['company']))
