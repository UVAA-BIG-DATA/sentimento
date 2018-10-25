from kafka import KafkaProducer
import time

startTime = time.time()

cleansedFile = open("cleansedData.txt", "r")
producer = KafkaProducer(bootstrap_servers='localhost:9092')  # 9092
x = 0

for jsonLine in cleansedFile:
    producer.send('tweets', key=bytes([x]), value=jsonLine.encode('utf-8'))
    x = x + 1

cleansedFile.close()

print (time.time() - startTime)