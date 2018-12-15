# Databricks notebook source
#Import functions
import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import MaxAbsScaler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.regression import GBTRegressor
from matplotlib.colors import ListedColormap, Normalize
from matplotlib.cm import get_cmap
from pyspark.ml.evaluation import RegressionEvaluator

# COMMAND ----------

#Loading tables
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
dfJoin.show(5)
dfFacebook = dfJoin.select('*').where(dfJoin.company == 'FACEBOOK')
dfAmazon = dfJoin.select('*').where(dfJoin.company == 'AMAZON')
dfGoogle = dfJoin.select('*').where(dfJoin.company == 'GOOGLE')
dfNetflix = dfJoin.select('*').where(dfJoin.company == 'NETFLIX')
dfSnapchat = dfJoin.select('*').where(dfJoin.company == 'SNAPCHAT')
dfMicrosoft = dfJoin.select('*').where(dfJoin.company == 'MICROSOFT')
dfFacebook.describe().toPandas().transpose()
display(dfFacebook)

# COMMAND ----------

#Feature scaling using MaxAbsScaler
vectorAssembler = VectorAssembler(inputCols = ['avg-sentiment','avg-followers','avg-volume'], outputCol = 'features')
v_dffacebook = vectorAssembler.transform(dfFacebook)
scaler = MaxAbsScaler(inputCol="features", outputCol="scaledFeatures")
scalerModel = scaler.fit(v_dffacebook)
scaledData = scalerModel.transform(v_dffacebook)
scaledData.select("features", "scaledFeatures").show()
v_dffacebook1 = scaledData.select(['features','scaledFeatures','avg-close'])
v_dffacebook1.show()

# COMMAND ----------

#Train test split
train_df,test_df = v_dffacebook1.randomSplit([0.8, 0.2])

# COMMAND ----------

#Linear Regression model
lr = LinearRegression(featuresCol = 'features', labelCol='avg-close', maxIter=10)
lr_model = lr.fit(train_df)
print("Coefficients: " + str(lr_model.coefficients))
print("Intercept: " + str(lr_model.intercept))
trainingSummary = lr_model.summary
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)

# COMMAND ----------

lr_predictions = lr_model.transform(test_df)
lr_predictions.select("prediction","avg-close","scaledFeatures")
from pyspark.ml.evaluation import RegressionEvaluator
lr_evaluator = RegressionEvaluator(predictionCol="prediction", \
                 labelCol="avg-close",metricName="r2")
print("R Squared (R2) on test data = %g" % lr_evaluator.evaluate(lr_predictions))

# COMMAND ----------

#Plot of Linear Regression
disp_pred = lr_predictions.select('avg-close','prediction')
display(disp_pred)

# COMMAND ----------

#Test set evaluation
test_result = lr_model.evaluate(test_df)
print("Root Mean Squared Error (RMSE) on test data = %g" % test_result.rootMeanSquaredError)

# COMMAND ----------

#Decision Tree Regression
dt = DecisionTreeRegressor(featuresCol ='features', labelCol = 'avg-close')
dt_model = dt.fit(train_df)
dt_predictions = dt_model.transform(test_df)
dt_evaluator = RegressionEvaluator(
    labelCol="avg-close", predictionCol="prediction", metricName="rmse")
rmse = dt_evaluator.evaluate(dt_predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

# COMMAND ----------

predictions_dt = dt_model.transform(test_df)
predictions_dt.select("prediction","avg-close","scaledFeatures").show()
#Plot of Decision Tree Regression 
disp_pred = predictions_dt.select('avg-close','prediction')
display(disp_pred)

# COMMAND ----------

#Decision Tree Regression with pruning
from pyspark.ml.regression import DecisionTreeRegressor
dt = DecisionTreeRegressor(featuresCol ='scaledFeatures', labelCol = 'avg-close', maxDepth=4)
dt_model = dt.fit(train_df)
dt_predictions = dt_model.transform(test_df)
dt_evaluator = RegressionEvaluator(
    labelCol="avg-close", predictionCol="prediction", metricName="rmse")
rmse = dt_evaluator.evaluate(dt_predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

# COMMAND ----------

predictions_dt_prune = dt_model.transform(test_df)
predictions_dt_prune.select("prediction","avg-close","scaledFeatures").show()
#Plot of Decision Tree Regression with Pruning
disp_pred = predictions_dt_prune.select('avg-close','prediction')
display(disp_pred)

# COMMAND ----------

#Gradient Boosting
gbt = GBTRegressor(featuresCol = 'features', labelCol = 'avg-close', maxIter=10,maxDepth=3)
gbt_model = gbt.fit(train_df)
gbt_predictions = gbt_model.transform(test_df)
gbt_predictions.select('prediction', 'avg-close', 'scaledFeatures').show(10)

# COMMAND ----------

#Plot of Gradient Boosting 
disp_pred = gbt_predictions.select('avg-close','prediction')
display(disp_pred)
gbt_evaluator = RegressionEvaluator(
    labelCol="avg-close", predictionCol="prediction", metricName="rmse")


# COMMAND ----------

rmse = gbt_evaluator.evaluate(gbt_predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

# COMMAND ----------

# from matplotlib.colors import ListedColormap, Normalize
# from matplotlib.cm import get_cmap
# cmap = get_cmap('YlOrRd')
import matplotlib.pyplot as plt
npts = 10
preds = [row['prediction'] for row in disp_pred.select('prediction').take(npts)]
labs = [row['avg-close'] for row in disp_pred.select('avg-close').take(npts)]
fig, ax = plt.subplots(1,1, figsize =(8,6))
ax.scatter(preds, labs, s=8**2)
ax.set_xlabel('Prediction')
ax.set_ylabel('Actual')
display(fig)

# COMMAND ----------

#Classification
df_fb = v_dffacebook1.toPandas()
df_fbmean = np.mean(df_fb["avg-close"])
class_labels = (df_fb['avg-close']>df_fbmean).astype(int)
df_fb = pd.concat([df_fb.features,df_fb.scaledFeatures,class_labels],axis=1)
df_fb = spark.createDataFrame(df_fb)
df_fb = df_fb.withColumnRenamed("avg-close","lowhigh")

# COMMAND ----------

train_df,test_df = df_fb.randomSplit([0.8, 0.2],seed=12345678)

# COMMAND ----------

# from pyspark.ml.classification import LogisticRegression
# from pyspark.ml.evaluation import MulticlassClassificationEvaluator
# lr = LogisticRegression(maxIter=10,labelCol="lowhigh", featuresCol="features")
# lrModel = lr.fit(train_df)
# trainingSummary = lrModel.summary
# objectiveHistory = trainingSummary.objectiveHistory
# print("objectiveHistory:")
# for objective in objectiveHistory:
#     print(objective)
# predictions = lrModel.transform(test_df)
# predictions.select("prediction", "lowhigh", "features").show(5)


# # Select (prediction, true label) and compute test error
# evaluator = MulticlassClassificationEvaluator(
#     labelCol="lowhigh", predictionCol="prediction", metricName="accuracy")
# accuracy = evaluator.evaluate(predictions)
# print("Accuracy= %g" % (accuracy))
# print("Test Error = %g" % (1.0 - accuracy))

# COMMAND ----------

from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
dt = DecisionTreeClassifier(labelCol="lowhigh", featuresCol="features",maxDepth=2)
model = dt.fit(train_df)

predictions = model.transform(test_df)
predictions.select("prediction", "lowhigh", "features").show(5)


# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="lowhigh", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Accuracy= %g" % (accuracy))
print("Test Error = %g" % (1.0 - accuracy))

# COMMAND ----------

from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
rf = RandomForestClassifier(labelCol="lowhigh", featuresCol="features", numTrees=10,maxDepth=3)
model = rf.fit(train_df)

predictions = model.transform(test_df)
predictions.select("prediction", "lowhigh", "features").show(5)

# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="lowhigh", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Accuracy= %g" % (accuracy))
print("Test Error = %g" % (1.0 - accuracy))

# COMMAND ----------

#GBT classifier
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
gbt = GBTClassifier(labelCol="lowhigh", featuresCol="features", maxIter=10,maxDepth=3)
model = gbt.fit(train_df)
predictions = model.transform(test_df)
predictions.select("prediction", "lowhigh", "features").show(5)

# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="lowhigh", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Accuracy= %g" % (accuracy))
print("Test Error = %g" % (1.0 - accuracy))
