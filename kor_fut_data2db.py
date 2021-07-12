import sys
import time
import pytz
import datetime as dt
from datetime import datetime
import pandas as pd
import numpy as np
import mysql.connector
from sqlalchemy import create_engine
import pymysql
from PyQt5.QtWidgets import *
from PyQt5.QAxContainer import *
from PyQt5.QtCore import *
import logging

pymysql.install_as_MySQLdb()

# read nh pwd, drpbx token from text file
with open('./nh_info.txt', 'r') as f:
    lines = f.readlines()
nhpwd = lines[0]

with open('./drpbx_info.txt', 'r') as f:
    lines = f.readlines()
drpbxtoken = lines[0]

# read db pwd from text file
with open('./db_info.txt', 'r') as f:
    lines = f.readlines()
dbpwd = lines[0]

# logger = logging.getLogger('trace')
# logger.setLevel(logging.INFO)
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# # log 출력
# stream_handler = logging.StreamHandler()
# stream_handler.setFormatter(formatter)
# logger.addHandler(stream_handler)
#
# # log를 파일에 출력
# file_handler = logging.FileHandler('data_recv.log')
# file_handler.setFormatter(formatter)
# logger.addHandler(file_handler)

# create connection instance
waidb = mysql.connector.connect(
    host='119.205.211.179',
    user='limpst',
    password=dbpwd,
    database='waikorea_port')
# create cursor instance
cursor = waidb.cursor()

class NH(QAxWidget):
    def __init__(self):
        super().__init__()
        self._create_nh_instance()
        self._set_signals_connect()
        self._set_signals_login()
        self._set_signals_disconnect()
        self._set_signals_recvrealdata()

    def _create_nh_instance(self):
        self.setControl("WRAX.WRAXCtrl.1")

    def _set_signals_connect(self):
        self.NetConnected.connect(self.server_connected)

    def _set_signals_disconnect(self):
        self.NetDisconnected.connect(self.disconnected)

    def _set_signals_login(self):
        self.ReplyLogin.connect(self.login_complete)

    def _set_signals_recvrealdata(self):
        self.RecvRealData.connect(self.recv_real_data)

    def connect_server(self):
        self.dynamicCall("OConnectHost(QString, QString)", '210.183.186.51', '8300')
        self.connect_event_loop = QEventLoop()
        self.connect_event_loop.exec_()

    def server_connected(self):
        if self:
            print("connected")
        else:
            print("not connected")
        self.connect_event_loop.exit()

    def disconnected(self):
        print('Disconnected!!')
        self.connect_event_loop.exit()

    def login(self):
        self.dynamicCall("OLogin(QString, QString, QString)", 'limpst', nhpwd, '')
        self.login_event_loop = QEventLoop()
        self.login_event_loop.exec_()

    def login_complete(self, code, msg):
        if code == 1:
            print('login completed')
        else:
            print('login failed : ' + msg)
        self.login_event_loop.exit()

    def reg_real_data(self, TrCode, KeyCode):

        reg_id = self.dynamicCall("ORegistRealData(QString, QString)", TrCode, KeyCode)

        # self.realdata_event_loop = QEventLoop()
        # self.realdata_event_loop.exec_()

        return reg_id

    def unreg_real_data(self, reg_id):

        check = self.dynamicCall("OUnregistRealData(int)", reg_id)

        return check

    def start_loop(self):

        self.realdata_event_loop = QEventLoop()
        self.realdata_event_loop.exec_()

    def recv_real_data(self, TrCode, KeyValue, RealID, DataSize, szData):

        if TrCode == 'SB_FUT_EXEC':

            exec_time, cur_p, ask_p, bid_p = get_price_info(szData)

            temp_df = pd.DataFrame({'exec_time': exec_time[:6],
                                    f'{dict_tickers[KeyValue.strip()]}_cur_p': cur_p,
                                    f'{dict_tickers[KeyValue.strip()]}_ask_p': ask_p,
                                    f'{dict_tickers[KeyValue.strip()]}_bid_p': bid_p}, index=[0])

            global df_out
            df_out = pd.concat([df_out, temp_df])

        elif TrCode == 'SB_FUT_HOGA':

            hoga_time, amts = get_amt_info(szData)

            temp_df2 = pd.DataFrame({'hoga_time': hoga_time[:6],
                                     f'{dict_tickers[KeyValue.strip()]}_ask5': amts[0],
                                     f'{dict_tickers[KeyValue.strip()]}_ask4': amts[1],
                                     f'{dict_tickers[KeyValue.strip()]}_ask3': amts[2],
                                     f'{dict_tickers[KeyValue.strip()]}_ask2': amts[3],
                                     f'{dict_tickers[KeyValue.strip()]}_ask1': amts[4],
                                     f'{dict_tickers[KeyValue.strip()]}_bid1': amts[5],
                                     f'{dict_tickers[KeyValue.strip()]}_bid2': amts[6],
                                     f'{dict_tickers[KeyValue.strip()]}_bid3': amts[7],
                                     f'{dict_tickers[KeyValue.strip()]}_bid4': amts[8],
                                     f'{dict_tickers[KeyValue.strip()]}_bid5': amts[9]}, index=[0])

            global df2_out
            df2_out = pd.concat([df2_out, temp_df2])

        self.realdata_event_loop.exit()


def get_price_info(szData):

    if szData.split()[0][4:] == '101R9000':
        cur_p = float(szData.split()[2])
        ask_p = float(szData.split()[11])
        bid_p = float(szData.split()[12])
    else:
        cur_p = float(szData.split()[2])
        ask_p = float(szData.split()[12])
        bid_p = float(szData.split()[13])

    exec_time = szData.split()[1]

    return exec_time, cur_p, ask_p, bid_p


def get_amt_info(szData):

    ask_1 = int(szData.split()[2].split('.')[1][2:8])
    bid_1 = int(szData.split()[2].split('.')[1][8:14])
    ask_2 = int(szData.split()[4].split('.')[1][2:8])
    bid_2 = int(szData.split()[4].split('.')[1][8:14])
    ask_3 = int(szData.split()[6].split('.')[1][2:8])
    bid_3 = int(szData.split()[6].split('.')[1][8:14])
    ask_4 = int(szData.split()[8].split('.')[1][2:8])
    bid_4 = int(szData.split()[8].split('.')[1][8:14])
    ask_5 = int(szData.split()[10].split('.')[1][2:8])
    bid_5 = int(szData.split()[10].split('.')[1][8:14])

    amts = [ask_5, ask_4, ask_3, ask_2, ask_1, bid_1, bid_2, bid_3, bid_4, bid_5]

    hoga_time = szData.split()[2].split('.')[1][14:]

    return hoga_time, amts


def insert_price_data_to_sql(df):

    df = df.replace(np.nan, 0.0)
    global today

    sql = f"INSERT INTO input_data (Year_Mon_Day, Hour_Min_Sec, KOSPI200, KOSPI200_ask, KOSPI200_bid, \
            USDKRW, USDKRW_ask, USDKRW_bid, NextUSD, NextUSD_ask, NextUSD_bid, KTB10, KTB10_ask, KTB10_bid, \
            KTB3, KTB3_ask, KTB3_bid) \
            VALUES ('{today[:4] + '/' + today[4:6] + '/' + today[6:]}', \
                    '{df['exec_time'].item()[:2] + ':' + df['exec_time'].item()[2:4] + ':' + df['exec_time'].item()[4:]}', \
                    {df['kospi_cur_p'].item()}, {df['kospi_ask_p'].item()}, {df['kospi_bid_p'].item()}, \
                    {df['usd_cur_p'].item()}, {df['usd_ask_p'].item()}, {df['usd_bid_p'].item()}, \
                    {df['nextusd_cur_p'].item()}, {df['nextusd_ask_p'].item()}, {df['nextusd_bid_p'].item()}, \
                    {df['ktb10_cur_p'].item()}, {df['ktb10_ask_p'].item()}, {df['ktb10_bid_p'].item()}, \
                    {df['ktb3_cur_p'].item()}, {df['ktb3_ask_p'].item()}, {df['ktb3_bid_p'].item()})"

    cursor.execute(sql)
    waidb.commit()


def insert_amt_data_to_sql(df):

    df = df.replace(np.nan, 0.0)
    global today

    sql = f"INSERT INTO input_data_amt (Year_Mon_Day, Hour_Min_Sec, \
            KOSPI200_ask5, KOSPI200_ask4, KOSPI200_ask3, KOSPI200_ask2, KOSPI200_ask1, \
            KOSPI200_bid1, KOSPI200_bid2, KOSPI200_bid3, KOSPI200_bid4, KOSPI200_bid5, \
            USDKRW_ask5, USDKRW_ask4, USDKRW_ask3, USDKRW_ask2, USDKRW_ask1, \
            USDKRW_bid1, USDKRW_bid2, USDKRW_bid3, USDKRW_bid4, USDKRW_bid5, \
            NextUSD_ask5, NextUSD_ask4, NextUSD_ask3, NextUSD_ask2, NextUSD_ask1, \
            NextUSD_bid1, NextUSD_bid2, NextUSD_bid3, NextUSD_bid4, NextUSD_bid5, \
            KTB10_ask5, KTB10_ask4, KTB10_ask3, KTB10_ask2, KTB10_ask1, \
            KTB10_bid1, KTB10_bid2, KTB10_bid3, KTB10_bid4, KTB10_bid5, \
            KTB3_ask5, KTB3_ask4, KTB3_ask3, KTB3_ask2, KTB3_ask1, \
            KTB3_bid1, KTB3_bid2, KTB3_bid3, KTB3_bid4, KTB3_bid5) \
            VALUES ('{today[:4] + '/' + today[4:6] + '/' + today[6:]}', \
                    '{df['hoga_time'].item()[:2] + ':' + df['hoga_time'].item()[2:4] + ':' + df['hoga_time'].item()[4:]}', \
                    {df['kospi_ask5'].item()}, {df['kospi_ask4'].item()}, {df['kospi_ask3'].item()}, {df['kospi_ask2'].item()}, {df['kospi_ask1'].item()}, \
                    {df['kospi_bid1'].item()}, {df['kospi_bid2'].item()}, {df['kospi_bid3'].item()}, {df['kospi_bid4'].item()}, {df['kospi_bid5'].item()}, \
                    {df['usd_ask5'].item()}, {df['usd_ask4'].item()}, {df['usd_ask3'].item()}, {df['usd_ask2'].item()}, {df['usd_ask1'].item()}, \
                    {df['usd_bid1'].item()}, {df['usd_bid2'].item()}, {df['usd_bid3'].item()}, {df['usd_bid4'].item()}, {df['usd_bid5'].item()}, \
                    {df['nextusd_ask5'].item()}, {df['nextusd_ask4'].item()}, {df['nextusd_ask3'].item()}, {df['nextusd_ask2'].item()}, {df['nextusd_ask1'].item()}, \
                    {df['nextusd_bid1'].item()}, {df['nextusd_bid2'].item()}, {df['nextusd_bid3'].item()}, {df['nextusd_bid4'].item()}, {df['nextusd_bid5'].item()}, \
                    {df['ktb10_ask5'].item()}, {df['ktb10_ask4'].item()}, {df['ktb10_ask3'].item()}, {df['ktb10_ask2'].item()}, {df['ktb10_ask1'].item()}, \
                    {df['ktb10_bid1'].item()}, {df['ktb10_bid2'].item()}, {df['ktb10_bid3'].item()}, {df['ktb10_bid4'].item()}, {df['ktb10_bid5'].item()}, \
                    {df['ktb3_ask5'].item()}, {df['ktb3_ask4'].item()}, {df['ktb3_ask3'].item()}, {df['ktb3_ask2'].item()}, {df['ktb3_ask1'].item()}, \
                    {df['ktb3_bid1'].item()}, {df['ktb3_bid2'].item()}, {df['ktb3_bid3'].item()}, {df['ktb3_bid4'].item()}, {df['ktb3_bid5'].item()})"

    cursor.execute(sql)
    waidb.commit()


def initialize_df():

    global df_out, df2_out
    df_out = pd.DataFrame(columns=['today', 'exec_time', 'kospi_cur_p', 'kospi_ask_p', 'kospi_bid_p',
                                   'usd_cur_p', 'usd_ask_p', 'usd_bid_p',
                                   'nextusd_cur_p', 'nextusd_ask_p', 'nextusd_bid_p',
                                   'ktb10_cur_p', 'ktb10_ask_p', 'ktb10_bid_p',
                                   'ktb3_cur_p', 'ktb3_ask_p', 'ktb3_bid_p'], index=[1])
    df2_out = pd.DataFrame(columns=['today', 'hoga_time',
                                    'kospi_ask5', 'kospi_ask4', 'kospi_ask3', 'kospi_ask2', 'kospi_ask1',
                                    'kospi_bid1', 'kospi_bid2', 'kospi_bid3', 'kospi_bid4', 'kospi_bid5',
                                    'usd_ask5', 'usd_ask4', 'usd_ask3', 'usd_ask2', 'usd_ask1',
                                    'usd_bid1', 'usd_bid2', 'usd_bid3', 'usd_bid4', 'usd_bid5',
                                    'nextusd_ask5', 'nextusd_ask4', 'nextusd_ask3', 'nextusd_ask2', 'nextusd_ask1',
                                    'nextusd_bid1', 'nextusd_bid2', 'nextusd_bid3', 'nextusd_bid4', 'nextusd_bid5',
                                    'ktb10_ask5', 'ktb10_ask4', 'ktb10_ask3', 'ktb10_ask2', 'ktb10_ask1',
                                    'ktb10_bid1', 'ktb10_bid2', 'ktb10_bid3', 'ktb10_bid4', 'ktb10_bid5',
                                    'ktb3_ask5', 'ktb3_ask4', 'ktb3_ask3', 'ktb3_ask2', 'ktb3_ask1',
                                    'ktb3_bid1', 'ktb3_bid2', 'ktb3_bid3', 'ktb3_bid4', 'ktb3_bid5'], index=[1])


def check_insert_data(datr, latest_data):

    # manipulate df in order to compare and save the latest price data to DB server
    if datr.shape[0] == 2 and datr.index[0] == 1:
        # delete first row which only has NaN values
        datr.drop(1, inplace=True)
        latest_data = datr.tail(1)
        # insert first data to sql server
        if len(datr.columns) <= 20:
            insert_price_data_to_sql(latest_data)
        else:
            insert_amt_data_to_sql(latest_data)

    # if datr.shape[0] == 2 and datr.index[0] == 0 and n == 0:
    #     latest_data = datr.tail(1)
    #     # insert last data to sql server
    #     if len(datr.columns) <= 20:
    #         insert_price_data_to_sql(latest_data)
    #     else:
    #         insert_amt_data_to_sql(latest_data)
    #     n += 1

    if datr.shape[0] == 3 and datr.index[0] == 1:
        datr.drop(1, inplace=True)
        temp_data = datr.head(1)
        latest_data = datr.tail(1)
        # insert last data to sql server
        if len(datr.columns) <= 20:
            insert_price_data_to_sql(temp_data)
            insert_price_data_to_sql(latest_data)
        else:
            insert_amt_data_to_sql(temp_data)
            insert_amt_data_to_sql(latest_data)

    # if datr.shape[0] == 3 and datr.index[0] == 0 and n == 0:
    #     latest_data = datr.tail(1)
    #     temp_data = datr.head(2).tail(1)
    #     # insert last data to sql server
    #     if len(datr.columns) <= 20:
    #         insert_price_data_to_sql(temp_data)
    #         insert_price_data_to_sql(latest_data)
    #     else:
    #         insert_amt_data_to_sql(temp_data)
    #         insert_amt_data_to_sql(latest_data)
    #     n += 1

    if datr.shape[0] >= 2:
        if len(datr.columns) <= 20:
            if latest_data['exec_time'].item() != datr.tail(1)['exec_time'].item():
                # change the last_data
                latest_data = df_out.tail(1)
                # insert the last data to sql server
                insert_price_data_to_sql(latest_data)
        else:
            if latest_data['hoga_time'].item() != datr.tail(1)['hoga_time'].item():
                # change the last_data
                latest_data = datr.tail(1)
                # insert the last data to sql server
                insert_amt_data_to_sql(latest_data)

    return datr, latest_data

# TR Code : 공통 호가 조회 (FZQ12011)
# 종목 Code : 코스피200 지수 최종거래일 in 9월 (101R9000)
# 종목 Code : USD/KRW 통화 최종거래일 in 7월 (175R7000) / 175년월000 (년 - R:2021, S:2022 / 월 2~9는 숫자 10,11,12는 A,B,C)
# 종목 Code : 국고채 10년물 최종거래일 in 9월 (167R9000)
# 종목 Code : 국고채 3년물 최종거래일 in 9월 (165R9000)
kospi = '101R9000'
usd = '175R7000'
nextusd = '175R8000'
ktb10 = '167R9000'
ktb3 = '165R9000'
tickers = [kospi, usd, nextusd, ktb10, ktb3]
dict_tickers = {'101R9000': 'kospi', '175R7000': 'usd', '175R8000': 'nextusd', '167R9000': 'ktb10', '165R9000': 'ktb3'}
global today
today = dt.datetime.now(pytz.timezone('Asia/Seoul')).strftime('%Y%m%d')

# path for saving file in Dropbox
dpb_path = 'C:\\Users\\waikorea\\Dropbox\\datafeed\\'
# path for saving file in local computer
local_path = 'C:\\Users\\waikorea\\Desktop\\NH\\CSV\\'

# create engine to connect to DB server
engine = create_engine("mysql+mysqldb://limpst:" + dbpwd + "@119.205.211.179/waikorea_port", encoding='utf-8')

# initialize DataFrame to save the data
global last_data, last_data2
last_data = pd.DataFrame()
last_data2 = pd.DataFrame()

# start the data per seconds collection program
if __name__ == "__main__":

    # create QApp object
    app = QApplication(sys.argv)

    # create QAxWidget object
    nh = NH()

    # connect to the server
    nh.connect_server()

    # log-in
    nh.login()

    start_t = '090000'  # start receiving data at 9:00
    end_t = '153459'    # end receiving data at 15:35

    reg_ids = []
    for ticker in tickers:
        reg_id = nh.reg_real_data('SB_FUT_EXEC', ticker)
        reg_id2 = nh.reg_real_data('SB_FUT_HOGA', ticker)
        reg_ids.append(reg_id)
        reg_ids.append(reg_id2)

    if len(reg_ids) == len(tickers)*2:
        print('registered for real-time data')
    else:
        print('some tickers have not been registered')

    # initialize dfs to store data
    initialize_df()

    # while loop for starting the data receiving event at 9:00
    while True:
        now = dt.datetime.now(pytz.timezone('Asia/Seoul')).strftime('%H%M%S')

        # start the process at 9:00
        if now >= start_t:

            # start the receiving event loop
            nh.start_loop()

            # initialize dfs every 5 seconds in order to reduce the process time
            if (int(now[4:]) % 5) == 0:
                initialize_df()

            # concat all the price data and delete duplicate lines
            df_out.drop_duplicates(subset=['exec_time'], inplace=True)
            # check and insert price data to DB server
            df_out, last_data = check_insert_data(df_out, last_data)

            # concat all the price data and delete duplicate lines
            df2_out.drop_duplicates(subset=['hoga_time'], inplace=True)
            # check and insert amt data to DB server
            df2_out, last_data2 = check_insert_data(df2_out, last_data2)

        # break the while loop at 15:35
        if now >= end_t:
            print('Market closed')
            break

    checks = 0
    for reg_id in reg_ids:
        check = nh.unreg_real_data(reg_id)
        if check:
            checks += 1

    if checks == len(reg_ids):
        print('unregistered complete')
    else:
        print('some ids are not unregistered yet')

    # read data from sql server and save it as csv file in both Dropbox and local computer
    price_data = pd.read_sql("input_data", con=engine, index_col='SEQ')
    price_data.to_csv(dpb_path + 'kor_fut_prices_' + today + '.csv', header=True, index=False)
    price_data.to_csv(local_path + 'kor_fut_prices_' + today + '.csv', header=True, index=False)
    amt_data = pd.read_sql("input_data_amt", con=engine, index_col='SEQ')
    amt_data.to_csv(dpb_path + 'kor_fut_amts_' + today + '.csv', header=True, index=False)
    amt_data.to_csv(local_path + 'kor_fut_amts_' + today + '.csv', header=True, index=False)

    time.sleep(5)

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
        sql_trunc_query = """TRUNCATE TABLE input_data"""
        sql_trunc_query2 = """TRUNCATE TABLE input_data_amt"""
        sql_query = [sql_trunc_query, sql_trunc_query2]
        # execute sql query
        for query in sql_query:
            cursor.execute(query)
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

    print('data collection task finished')
