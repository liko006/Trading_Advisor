import time
import pytz
import datetime as dt
import pandas as pd
import numpy as np
import mysql.connector
from sqlalchemy import create_engine
import pymysql

pymysql.install_as_MySQLdb()


def signal_is(last1, last2):
    # ticker in uptrend
    if last2 == 3:
        # uptrend has changed to downtrend
        if last1 - last2 <= -4:
            return -1   # give sell signal
        else:
            return 0
    if last2 == 1:
        # uptrend has changed to downtrend
        if last1 - last2 <= -2:
            return -1   # give sell signal
        else:
            return 0

    # ticker in downtrend
    if last2 == -1:
        # downtrend has changed to uptrend
        if last1 - last2 >= 2:
            return 1   # give buy signal
        else:
            return 0
    if last2 == -3:
        # downtrend has changed to uptrend
        if last1 - last2 >= 4:
            return 1   # give buy signal
        else:
            return 0


# assign datetime of today
today = dt.datetime.now(pytz.timezone('Asia/Seoul')).strftime('%Y%m%d')

# create engine to connect to DB server
engine = create_engine("mysql+mysqldb://limpst:" + "waikoreat2" + "@119.205.211.179/waikorea_port", encoding='utf-8')

# create DataFrame for storing signal data
df_signal = pd.DataFrame(columns=['Time', 'kospi_Signal', 'usd_Signal'])

'''
while loop start
every 6 secs, read sql data and generate buy/sell signal for each ticker
and store the signal data into another DataFrame
'''

# check for every 12 second
now = dt.datetime.now(pytz.timezone('Asia/Seoul')).strftime('%H%M%S')
nowsec = int(now[4:])

# read sql table data and save it into DataFrame
output_data = pd.read_sql("output_data", con=engine, index_col='SEQ', parse_dates='CRE_DATE')

# sort output data by time
output_data = output_data.sort_values("CRE_DATE")

# divide data by tickers
kospi_data = output_data[["CRE_DATE", "KOSPI200"]]
usd_data = output_data[["CRE_DATE", "USDKRW"]]

# drop NaN
kospi_data = kospi_data.dropna()
usd_data = usd_data.dropna()

# generate buy/sell signals
kospi_signal = signal_is(kospi_data.iloc[-1, 1], kospi_data.iloc[-2, 1])
usd_signal = signal_is(usd_data.iloc[-1, 1], usd_data.iloc[-2, 1])

# append signals to df
df_signal = df_signal.append(pd.Series([now, kospi_signal, usd_signal], index=df_signal.columns), ignore_index=True)


print(kospi_data[-2:])
print(kospi_signal)
print('--------------')
print(usd_data[-2:])
print(usd_signal)
print('--------------')
print(df_signal)
print(df_signal.tail(1).iloc[0, 1:].sum())
for col in df_signal.columns.tolist()[1:]:
    print(col.split('_')[0])
    print(type(col.split('_')[0]))
    print(type(df_signal.tail(1)[col].item()))