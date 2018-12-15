# Databricks notebook source
import matplotlib.pyplot as plt
from pyspark.sql import functions as F
import matplotlib.mlab as mlab
from matplotlib.ticker import MaxNLocator
from pyspark.ml.feature import VectorAssembler
from mpl_toolkits.mplot3d import Axes3D
%matplotlib inline
from pyspark.ml.feature import MaxAbsScaler
from pyspark.ml.clustering import KMeans

# COMMAND ----------

dfTwitter = spark.read.json('/FileStore/tables/TwitterData.json')
dfStock = spark.read.format("csv").option("header", "true").load("/FileStore/tables/*.csv")

dfAvgSent = dfTwitter.groupby('tweet_hour', 'company').agg(F.mean('sentiment'), F.mean('followers_count'))
dfAvgSent = dfAvgSent.withColumnRenamed('company', 'comp')
dfAvgStock = dfStock.groupby('stock_hour', 'company').agg(F.mean('close'), F.mean('volume'))

dfJoin = dfAvgSent.join(dfAvgStock, (dfAvgSent.comp == dfAvgStock.company) & (dfAvgSent.tweet_hour == dfAvgStock.stock_hour+5))
dfJoin = dfJoin.withColumnRenamed("avg(sentiment)","avg-sentiment")
dfJoin = dfJoin.withColumnRenamed("avg(close)","avg-close")
dfJoin = dfJoin.withColumnRenamed("avg(volume)","avg-volume")
dfJoin = dfJoin.withColumnRenamed("avg(followers_count)","avg-followers")
dfJoin.show()

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
dfJoin1 = dfJoin.select("avg-sentiment","avg-followers","avg-volume")
inputFeatures = ["avg-sentiment","avg-followers","avg-volume"]
assembler = VectorAssembler(inputCols=inputFeatures, outputCol="features")
dfJoin2 = assembler.transform(dfJoin1)

# COMMAND ----------

# Scaling features
scaler = MaxAbsScaler(inputCol="features", outputCol="scaledFeatures")
scalerModel = scaler.fit(dfJoin2)
scaledData = scalerModel.transform(dfJoin2)
scaledData.select("features", "scaledFeatures").show()

# COMMAND ----------

#Elbow method
import numpy as np
cost = np.zeros(10)
for k in range(2,10):
    kmeans = KMeans().setK(k).setFeaturesCol("scaledFeatures").setPredictionCol("prediction").setMaxIter(1).setSeed(1)
    model = kmeans.fit(scaledData)
    cost[k] = model.computeCost(scaledData)

# COMMAND ----------

#Plot of elbow method
fig, ax = plt.subplots(1,1, figsize =(8,6))
ax.plot(range(2,10),cost[2:10])
ax.set(title='Elbow method to predict number of clusters')
ax.set_xlabel('k')
ax.set_ylabel('cost')
ax.xaxis.set_major_locator(MaxNLocator(integer=True))
plt.show()
display(fig)

# COMMAND ----------

#Kmeans algorithm
kmeans = KMeans().setK(8).setFeaturesCol("scaledFeatures").setPredictionCol("prediction").setMaxIter(10).setSeed(1)
model = kmeans.fit(scaledData)
output_df = model.transform(scaledData)
display(output_df.take(5))
groupedByRegion = output_df.groupby(output_df['prediction']).count()
display(groupedByRegion)

# COMMAND ----------

df_pred = output_df.select('avg-sentiment','avg-followers','avg-volume','prediction')
df_pandas = df_pred.toPandas()

# COMMAND ----------

image = plt.figure(figsize=(12,10)).gca(projection='3d')
image.scatter(df_pandas["avg-sentiment"], df_pandas["avg-followers"], df_pandas["avg-volume"], c=df_pandas.prediction)
image.set_xlabel('avg-sentiment')
image.set_ylabel('avg-followers')
image.set_zlabel('avg-volume')
plt.show()
display()
