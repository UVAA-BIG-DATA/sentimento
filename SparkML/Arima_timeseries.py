# Databricks notebook source
#Import Functions
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.functions import to_date, to_timestamp
import math

# COMMAND ----------

import requests, pandas as pd, numpy as np
from pandas import DataFrame
from io import StringIO
import time, json
from datetime import date
from statsmodels.tsa.stattools import adfuller, acf, pacf
from statsmodels.tsa.arima_model import ARIMA
from statsmodels.tsa.seasonal import seasonal_decompose
from sklearn.metrics import mean_squared_error
import matplotlib.pylab as plt
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf

# COMMAND ----------

dfStock = spark.read.format("csv").option("header", "true").load("/FileStore/tables/*.csv")
# dfStock.show()

# COMMAND ----------

dfNetflix = dfStock.select('*').where(dfStock.company == 'NETFLIX')
dfNetflix = dfNetflix.select('*').where(dfStock.stock_date == '101918')
dfNetflix1 = dfNetflix.toPandas()
dfNetflix2 = dfNetflix1.sort_values(['timestamp'])
dfNetflix3 = dfNetflix2[['close','timestamp']]
dfNetflix3['timestamp'] = pd.to_datetime(dfNetflix3['timestamp'])
dfNetflix3 = dfNetflix3.set_index('timestamp')

# COMMAND ----------

#Train test split
train = dfNetflix3[:int(0.8*(len(dfNetflix3)))]
test = dfNetflix3[int(0.8*(len(dfNetflix3))):]
# train.head(5)

# COMMAND ----------

train1 = spark.createDataFrame(train)
train2 = train1.select("close")
from pyspark.sql.types import IntegerType
train2 = train2.withColumn("close",train2["close"].cast("float"))
train3 = train2.toPandas()
ts = train3.close

# COMMAND ----------

test1 = spark.createDataFrame(test)
test2 = test1.select("close")
from pyspark.sql.types import IntegerType
test2 = test2.withColumn("close",test2["close"].cast("float"))
test3 = test2.toPandas()
ts_test = test3.close

# COMMAND ----------

fig, ax = plt.subplots()
plt.plot(train.close)
display(fig)

# COMMAND ----------

def test_stationarity(df):    
    dftest = adfuller(df, autolag='AIC')
    dfoutput = pd.Series(dftest[0:4], index=['Test Statistic','p-value','#Lags Used','Number of Observations Used'])
    for key,value in dftest[4].items():
        dfoutput['Critical Value (%s)'%key] = value
    print(dfoutput)

# COMMAND ----------

test_stationarity(train3.close)

# COMMAND ----------

ts_diff = ts - ts.shift()
ts_diff.drop(ts_diff.index[0], inplace=True)
fig, ax = plt.subplots()
plt.plot(ts_diff)
display(fig)

# COMMAND ----------

test_stationarity(ts_diff)

# COMMAND ----------

acf1 = plot_acf(ts_diff, lags=10)
display(acf1)

# COMMAND ----------

pacf1 = plot_pacf(ts_diff, lags=10)
display(pacf1)

# COMMAND ----------

model = ARIMA(ts_diff.values, order=(0, 1, 0)) 
results_ARIMA = model.fit(maxiter=10)  
print(results_ARIMA.summary())

# COMMAND ----------

fitted_values  = results_ARIMA.predict(1,len(test3)-1,typ = "linear")
fitted_values1 = fitted_values + ts_test.shift()[1:]
fig, ax = plt.subplots()
plt.plot(fitted_values1)
plt.plot(ts_test)
display(fig)

# COMMAND ----------

train_test = pd.concat([train3,test3])
fitted_values1 = pd.DataFrame(fitted_values1)
df = pd.DataFrame([337],columns = ["close"])
train_fitted1 = pd.concat([train3,df, fitted_values1])
train_test = train_test.reset_index()
train_fitted1 = train_fitted1.reset_index()

# COMMAND ----------

figure, ax = plt.subplots()
ax.plot(train_fitted1.close,label='Predicted')
ax.plot(train_test.close,label='Actual')
ax.set(title='Actual vs Predicted Netflix Stock Price', xlabel='Timestamp equivalent to 5 min interval from 9:30 am to 4 pm', ylabel='Stock Price')
legend = ax.legend(loc='upper right')
display(figure)
