import sys
import time
import pytz
import datetime as dt
import pandas as pd
import numpy as np
import mysql.connector
from sqlalchemy import create_engine
import pymysql

pymysql.install_as_MySQLdb()

# read db pwd from text file
with open('./db_info.txt', 'r') as f:
    lines = f.readlines()
dbpwd = lines[0]

# path of the file's location
path = 'C:\\Users\\waikorea\\Desktop\\NH\\CSV\\'

# assign datetime of today
today = dt.datetime.now(pytz.timezone('Asia/Seoul')).strftime('%Y%m%d')

# create engine to connect to DB server
engine = create_engine("mysql+mysqldb://limpst:" + dbpwd + "@119.205.211.179/waikorea_port", encoding='utf-8')

# start the data transfer program
if __name__ == "__main__":

    start_t = '090009'  # start receiving data at 9:00
    end_t = '153521'  # end receiving data at 15:35

    daily_data = None
    daily_data_update = None

    while True:
        now = dt.datetime.now(pytz.timezone('Asia/Seoul')).strftime('%H%M%S')

        if now < start_t:
            continue

        if now >= start_t:

            if int(now[4:]) % 10 == 0:

                if daily_data is None:
                    # load first data from saved csv file
                    try:
                        daily_data = pd.read_csv(path + today + '.csv')
                    except pd.errors.EmptyDataError as e:
                        time.sleep(1)
                        daily_data = pd.read_csv(path + today + '.csv')
                    # change the index name
                    daily_data.index.name = 'SEQ'
                else:
                    # load updated data from saved csv file
                    try:
                        daily_data_update = pd.read_csv(path + today + '.csv')
                    except pd.errors.EmptyDataError as e:
                        time.sleep(1)
                        daily_data_update = pd.read_csv(path + today + '.csv')
                    # change the index name
                    daily_data_update.index.name = 'SEQ'

                # concat existing data with newly loaded data
                concat_data = pd.concat([daily_data, daily_data_update], axis=0)
                # select data which are need to be updated to DB table
                data_to_update = concat_data.drop_duplicates(subset=['Hour_Min_Sec'], keep=False)
                # renew the loaded data
                daily_data = concat_data.drop_duplicates(subset=['Hour_Min_Sec'], keep='first')
                # update selected data to DB table
                data_to_update.to_sql('input_data_csv', con=engine, if_exists='append')

                time.sleep(0.5)

        # # test to run the program until any specified time
        # if now >= '170201':
        #     break

        # break the while loop at 15:35
        if now >= end_t:
            print('Market closed')
            break

    print('data transfer finished')

    while True:
        now = dt.datetime.now(pytz.timezone('Asia/Seoul')).strftime('%H%M%S')

        if now >= '153601':
            # connect to DB and delete the sql table data when the market closes
            try:
                # create connection instance
                waidb = mysql.connector.connect(
                    host='119.205.211.179',
                    user='limpst',
                    password=dbpwd,
                    database='waikorea_port')
                # create cursor instance
                cursor = waidb.cursor()
                # write sql query
                sql_trunc_query = """TRUNCATE TABLE input_data_csv"""
                # execute sql query
                cursor.execute(sql_trunc_query)
                waidb.commit()
                print('SQL table cleared')

            # raise error if there is any
            except mysql.connector.Error as e:
                print(e)

            # close the DB connection
            finally:
                if waidb.is_connected():
                    cursor.close()
                    waidb.close()
                    print('connection closed')
                    break

    print('task finished')
