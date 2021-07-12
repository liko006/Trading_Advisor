# Using python-binance library
import os
import time
import pandas as pd
import numpy as np
import datetime as dt
import pytz
from datetime import datetime
import mysql.connector
from sqlalchemy import create_engine
import pymysql
from binance.client import Client
from binance.streams import ThreadedWebsocketManager

pymysql.install_as_MySQLdb()

# read api key from text file
with open('./info.txt', 'r') as f:
    lines = f.readlines()
keys = ''.join(lines)
keys = keys.split('\n')
my_api_key = keys[0]
my_secret_key = keys[1]

# read db pwd from text file
with open('./db_info.txt', 'r') as f:
    lines = f.readlines()
dbpwd = lines[0]

# Binance API key
api_key = my_api_key
secret_key = my_secret_key



# spot data collecting func
def get_trade_data(msg):
    ''' define how to process incoming WebSocket messages '''

    tick = msg['data']['s'].lower()[:-4]

    sql = f"INSERT INTO {tick}_spot (symbol, date, time, price, ask, bid) \
            VALUES ('{msg['data']['s']}', \
                    '{datetime.fromtimestamp(msg['data']['E']/1000).strftime('%Y/%m/%d')}', \
                    '{datetime.fromtimestamp(msg['data']['E']/1000).strftime('%H:%M:%S')}', \
                    {round(float(msg['data']['c']),3)}, \
                    {round(float(msg['data']['a']),3)}, \
                    {round(float(msg['data']['b']),3)})"

    mc.execute(sql)
    mydb.commit()


# connect to DB
mydb = mysql.connector.connect(
    host='119.205.211.179',
    user='hlee',
    password=dbpwd,
    database='leverj_orderbook'
)

mc = mydb.cursor()

# init and start the WebSocket
twm = ThreadedWebsocketManager(api_key=api_key, api_secret=secret_key)

twm.start()

streams = ['btcusdt@ticker', 'ethusdt@ticker', 'ltcusdt@ticker', 'dotusdt@ticker', 'linkusdt@ticker']

conn_key = twm.start_multiplex_socket(get_trade_data, streams=streams)

while True:
    now = dt.datetime.now(pytz.timezone('Asia/Seoul')).strftime('%H%M%S')

    if now > '150000':
        # stop websocket
        twm.stop()
        break

time.sleep(1)

# read sql data from DB
engine = create_engine("mysql+mysqldb://hlee:" + dbpwd + "@119.205.211.179/leverj_orderbook",
                       encoding='utf-8')

btc_data = pd.read_sql("btc_spot", con=engine, index_col='SEQ')
eth_data = pd.read_sql("eth_spot", con=engine, index_col='SEQ')
ltc_data = pd.read_sql("ltc_spot", con=engine, index_col='SEQ')
dot_data = pd.read_sql("dot_spot", con=engine, index_col='SEQ')
link_data = pd.read_sql("link_spot", con=engine, index_col='SEQ')

# save data in csv file
path = 'C:\\Users\\waikorea\\Dropbox\\crypto\\'
today = dt.datetime.now(pytz.timezone('Asia/Seoul')).strftime('%Y%m%d')
btc_data.to_csv(path + 'btc_spot_' + today + '.csv', header=True, index=False)
eth_data.to_csv(path + 'eth_spot_' + today + '.csv', header=True, index=False)
ltc_data.to_csv(path + 'ltc_spot_' + today + '.csv', header=True, index=False)
dot_data.to_csv(path + 'dot_spot_' + today + '.csv', header=True, index=False)
link_data.to_csv(path + 'link_spot_' + today + '.csv', header=True, index=False)

# clear sql tables
try:
    # write sql query
    sql_trunc_query = """TRUNCATE TABLE btc_spot"""
    sql_trunc_query2 = """TRUNCATE TABLE eth_spot"""
    sql_trunc_query3 = """TRUNCATE TABLE ltc_spot"""
    sql_trunc_query4 = """TRUNCATE TABLE dot_spot"""
    sql_trunc_query5 = """TRUNCATE TABLE link_spot"""
    sql_trunc = [sql_trunc_query, sql_trunc_query2, sql_trunc_query3,
                 sql_trunc_query4, sql_trunc_query5]
    # execute sql query
    for query in sql_trunc:
        mc.execute(query)
        mydb.commit()
    print('SQL tables cleared')

# raise error if there is any
except mysql.connector.Error as e:
    print(e)

# close the DB connection
finally:
    if mydb.is_connected():
        mc.close()
        mydb.close()
        print('connection closed')