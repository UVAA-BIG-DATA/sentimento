from textblob import TextBlob
from kafka import KafkaProducer
import pandas as pd
import json
import re

producer = KafkaProducer(bootstrap_servers='localhost:9092')  # 9092

# Reading the json
with open('cleansedData.txt') as file:
    data = []
    for line in file:
        data.append(json.loads(line))

# Sentiment using TextBlob
df = pd.DataFrame(data)
df1 = df.copy()

sentiment = []
for i in df.text:
    blob = TextBlob(i)
    sentiment.append(blob.sentiment.polarity)
df1['sentiment'] = pd.Series(sentiment)


def urls(s):
    url = re.findall(r'(https?://\S+)', s)
    return url


def element(list1):
    if len(list1) > 0:
        return list1[0]
    else:
        pass


TechList=["Facebook", "Microsoft", "Google", "Amazon", "Netflix", "Snapchat"]
pat = '|'.join(r"\b{}\b".format(x) for x in TechList)
df1['company'] = df1.text.str.findall(pat, flags=re.IGNORECASE).apply(lambda x: list(set(x)))
df1.company = df1.company.apply(element)

df1new = df1.loc[~df1.company.isnull()]
df1new['company'] = map(lambda x: x.upper(), df1new['company'])

df1new.to_json("cleanedDataWithSentiment.txt", orient="records")

x = 0
for row in df1new.to_dict(orient='records'):
    val = json.dumps(row)
    producer.send('tweetSA', key=bytes([x]), value=val.encode('utf-8'))
