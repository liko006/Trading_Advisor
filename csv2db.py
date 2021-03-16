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

# path for loading file
path = 'path of the location of the file'

# assign datetime of today
today = dt.datetime.now(pytz.timezone('Asia/Seoul')).strftime('%Y%m%d')

# create engine to connect to DB server
engine = create_engine("mysql+mysqldb://limpst:"+"waikoreat2"+"@119.205.211.179/waikorea_port", encoding='utf-8')

# start the data transfer program
if __name__ == "__main__":

    start_t = '090009'  # start receiving data at 9:00
    end_t = '153521'    # end receiving data at 15:35

    while True:
        now = dt.datetime.now(pytz.timezone('Asia/Seoul')).strftime('%H%M%S')

        if now < start_t:
            continue

        # start the process at 9:00
        if now == start_t:
            print('start data transfer')

        if now >= start_t:

            if int(now[4:]) % 10 == 0:
                # load data from saved csv file
                daily_data = pd.read_csv(path + today + '.csv')
                # save data to DB table
                daily_data.to_sql('input_data_csv', con=engine, if_exists='replace')

        # # test to run the program until any specified time
        # if now >= '153901':
        #     break

        # break the while loop at 15:35
        if now >= end_t:
            print('Market closed')
            break

    print('data transfer finished')

    if now >= '153600':
        # connect to DB and delete the sql table data when the market closes
        try:
            # create connection instance
            waidb = mysql.connector.connect(
                host='host',
                user='username',
                password='password',
                database='database name')
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
