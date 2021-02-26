import sys
import time
import pytz
import datetime as dt
import pandas as pd
import numpy as np
# import dropbox
from PyQt5.QtWidgets import *
from PyQt5.QAxContainer import *
from PyQt5.QtCore import *


class NH(QAxWidget):
    def __init__(self):
        super().__init__()
        self._create_nh_instance()
        self._set_signals_connect()
        self._set_signals_login()
        self._set_signals_recvdata()
        self._set_signals_disconnect()

    def _create_nh_instance(self):
        self.setControl("WRAX.WRAXCtrl.1")

    def _set_signals_connect(self):
        self.NetConnected.connect(self.server_connected)

    def _set_signals_disconnect(self):
        self.NetDisconnected.connect(self.disconnected)

    def _set_signals_login(self):
        self.ReplyLogin.connect(self.login_complete)

    def _set_signals_recvdata(self):
        self.RecvData.connect(self.recv_data)

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

    def login(self):
        self.dynamicCall("OLogin(QString, QString, QString)", 'ID', 'PW', '')
        self.login_event_loop = QEventLoop()
        self.login_event_loop.exec_()

    def login_complete(self, code):
        if code == 1:
            print('login completed')
        else:
            print('login failed')
        self.login_event_loop.exit()

    def req_data(self, ticker):
        self.dynamicCall("ORequestData(QString, int, QString, bool, bool, int)", 'FZQ12011', len(ticker), ticker, False, False, 60)

        # get the time when data were requested
        x = dt.datetime.now(pytz.timezone('Asia/Seoul'))
        y = x.strftime('%Y/%m/%d')
        z = x.strftime('%H:%M:%S')

        self.reqdata_event_loop = QEventLoop()
        self.reqdata_event_loop.exec_()

        # return date and time
        return y, z

    def recv_data(self, DataType, TrCode, RqID, DataSize, Data):

        # store the current price at received time
        cur_price = Data[160:180].strip()
        name = Data[:8]
        # append each data (received time, price) to DataFrame
        global df2
        if name == kospi and bool(cur_price):
            df2 = df2.append(pd.Series([cur_price, np.nan, np.nan], index=df2.columns), ignore_index=True)
        elif name == usd and bool(cur_price):
            df2 = df2.append(pd.Series([np.nan, cur_price, np.nan], index=df2.columns), ignore_index=True)
        elif name == ktb and bool(cur_price):
            df2 = df2.append(pd.Series([np.nan, np.nan, cur_price], index=df2.columns), ignore_index=True)

        self.reqdata_event_loop.exit()


# initialize DataFrame to save the data
global df1, df2
df1 = pd.DataFrame(columns=['Year/Mon/day', 'Hour:Min:Sec'])
df2 = pd.DataFrame(columns=['KOSPI200', 'USDKRW', 'KTB10'])


# TR Code : 공통 호가 조회 (FZQ12011)
# 종목 Code : 코스피200 지수 3월 만기 (101R3000)
# 종목 Code : USD/KRW 통화 3월 만기 (175R3000) / 175년월000 (년 - R:2021, S:2022 / 월 2~9는 숫자 10,11,12는 A,B,C)
# 종목 Code : 국고채 10년물 최종거래일 3월 (167R3000)
kospi = '101R3000'
usd = '175R3000'
ktb = '167R3000'
tickers = [kospi, usd, ktb]
today = dt.datetime.now(pytz.timezone('Asia/Seoul')).strftime('%Y%m%d')

# Dropbox access token
# access_token = 'TOKEN'
# dbx = dropbox.Dropbox(access_token, timeout=None)

# path for saving file
path = 'C:\\Users\\waikorea\\Dropbox\\datafeed\\'

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
    end_t = '153511'    # end receiving data at 15:35

    while True:
        now = dt.datetime.now(pytz.timezone('Asia/Seoul')).strftime('%H%M%S')

        if now < start_t:
            continue

        # start the process at 9:00
        if now >= start_t:
            # request the data and receive it from API server
            for ticker in tickers:
                y, z = nh.req_data(ticker)
                # append requested time
                df1 = df1.append(pd.Series([y, z], index=df1.columns), ignore_index=True)
                # concatenate df1 and df2 (date and time + price)
                df = pd.concat([df1, df2], axis=1)
                # wait 1 second for not violating the amount limit for accessing server data
                time.sleep(1)

            # save collected data to csv file at given path for every 6 seconds
            if int(now[4:]) % 6 == 0:
                df.to_csv(path + today + '.csv', header=True, index=False)

                # upload the csv file to dropbox folder
                # filename = path + today+'.csv'
                # pathname = "/datafeed/" + today+ '.csv'
                # with open(filename, "rb") as f:
                #     dbx.files_upload(f.read(), pathname, mode=dropbox.files.WriteMode.overwrite)

        # test to run the program until any specified time
        # if now >= '165900':
        #     break

        # break the while loop at 15:35
        if now >= end_t:
            break

    # # concatenate df1 and df2 (date and time + price)
    #df_full = pd.concat([df1, df2], axis=1)

    # # if there are no data at certain second, fill the previous data
    # df_full.fillna(method='ffill', inplace=True)

    # # drop duplicate rows if there is any
    # df_full.drop_duplicates(subset=['Year/Mon/day', 'Hour:Min:Sec'], inplace=True)

    # # save fully collected data to csv file at given path
    # df_full.to_csv(path + today + '_full.csv', header=True, index=False)
 
    # # upload the csv file to dropbox folder
    # filename = path + today + '_full.csv'
    # pathname = "/datafeed/" + today + '_full.csv'
    # with open(filename, "rb") as f:
    #     dbx.files_upload(f.read(), pathname, mode=dropbox.files.WriteMode.overwrite)

    print('data collection finished')
