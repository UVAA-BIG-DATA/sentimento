import time
from cassandra.cluster import Cluster
from kafka import KafkaConsumer

cluster = Cluster()
while session is NULL:
    try:
        session = cluster.connect("stock")
    except:
        print("Error connecting Cassandra")
        time.sleep(5)

consumer = KafkaConsumer(
  'stock',
  auto_offset_reset='earliest',
  bootstrap_servers=['localhost:9092'],
  api_version=(0, 10),
  consumer_timeout_ms=1000
)

for msg in consumer:
    session.execute(
      """
      INSERT INTO stock
      (company, timestamp, open, high, low, close, volume)
      VALUES (%s, %s, %s, %s, %s, %s, %s);
      """, msg.split(","))