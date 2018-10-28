
# coding: utf-8

# In[90]:

import pandas as pd
import numpy as np
import datetime
from datetime import datetime

filename = "C:\Users\Ujval\Downloads\Academia\Fall 2018\Big Data\project\Facebook.csv"
df = pd.read_csv(filename)
df['stock_dates'] =pd.to_datetime(df.timestamp)
df['stock_date'] = df.stock_dates.apply(lambda x:x.date().strftime("%m%d%y"))
df['stock_hour'] = df.stock_dates.dt.strftime("%m%d%y%H")
df = df.drop('stock_dates',axis=1)
df.to_csv("FACEBOOK.csv")

