from cassandra.cluster import Cluster
from kafka import KafkaConsumer
import json

cluster = Cluster()
session = cluster.connect("tweet")
consumer = KafkaConsumer(
  'tweet',
  auto_offset_reset='earliest',
  bootstrap_servers=['localhost:9092'],
  api_version=(0, 10),
  consumer_timeout_ms=1000
)