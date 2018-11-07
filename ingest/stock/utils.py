import pandas as pd
import numpy as np
import datetime
from datetime import datetime


def dedup(text):
    if isinstance(text, list) is False:
        text = text.split('\n')
    return list(set(text))


def clean(text):
    if isinstance(text, list) is False:
        text = text.split('\n')
    text = filter(lambda line: 'Information' not in line and '{' not in line, text)
    return [line.strip() for line in list(text)]


def sort(text):
    if isinstance(text, list) is False:
        text = text.split('\n')


def appendCol(text, colname=''):
    if isinstance(text, list) is False:
        text = text.split('\n')
    return ['%s,%s' % (colname, line) for line in text[:]]


def extract(filename):
    df = pd.read_csv(filename)
    df['stock_dates'] = pd.to_datetime(df.timestamp)
    df['stock_date'] = df.stock_dates.apply(
        lambda x: x.date().strftime("%m%d%y")
    )
    df['stock_hour'] = df.stock_dates.dt.strftime("%m%d%y%H")
    df = df.drop('stock_dates', axis=1)
    df.to_csv(filename)
