import sys
import time
import pytz
import datetime as dt
import pandas as pd
import numpy as np
from PyQt5.QtWidgets import *
from PyQt5.QAxContainer import *
from PyQt5.QtCore import *
import logging

# read nh pwd, drpbx token from text file
with open('./nh_info.txt', 'r') as f:
    lines = f.readlines()
nhpwd = lines[0]

with open('./drpbx_info.txt', 'r') as f:
    lines = f.readlines()
drpbxtoken = lines[0]

logger = logging.getLogger('trace')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# # log 출력
# stream_handler = logging.StreamHandler()
# stream_handler.setFormatter(formatter)
# logger.addHandler(stream_handler)
#
# # log를 파일에 출력
# file_handler = logging.FileHandler('data_recv.log')
# file_handler.setFormatter(formatter)
# logger.addHandler(file_handler)


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
        ask_bid_price = Data.split()[-21:-19]
        ask_bid_amt = Data.split()[-34:-6:3]
        name = Data[:8]

        # append each data (received time, price) to DataFrame
        global df2, df3, df4
        if name == kospi and bool(cur_price):
            df2 = df2.append(pd.Series([cur_price, np.nan, np.nan, np.nan, np.nan], index=df2.columns), ignore_index=True)
            df3, df4 = append_data(ask_bid_price, ask_bid_amt)
        elif name == usd and bool(cur_price):
            df2 = df2.append(pd.Series([np.nan, cur_price, np.nan, np.nan, np.nan], index=df2.columns), ignore_index=True)
            df3, df4 = append_data(ask_bid_price, ask_bid_amt)
        elif name == nextusd and bool(cur_price):
            ask_bid_price, ask_bid_amt = check_price_amts(ask_bid_price, ask_bid_amt, Data)
            df2 = df2.append(pd.Series([np.nan, np.nan, cur_price, np.nan, np.nan], index=df2.columns), ignore_index=True)
            df3, df4 = append_data(ask_bid_price, ask_bid_amt)
        elif name == ktb10 and bool(cur_price):
            df2 = df2.append(pd.Series([np.nan, np.nan, np.nan, cur_price, np.nan], index=df2.columns), ignore_index=True)
            df3, df4 = append_data(ask_bid_price, ask_bid_amt)
        elif name == ktb3 and bool(cur_price):
            df2 = df2.append(pd.Series([np.nan, np.nan, np.nan, np.nan, cur_price], index=df2.columns), ignore_index=True)
            df3, df4 = append_data(ask_bid_price, ask_bid_amt)

        self.reqdata_event_loop.exit()


def check_price_amts(ask_bid_price, ask_bid_amt, Data):

    ask_bid_amt_temp = []

    if (Data.split()[10] != '-') and (Data.split()[10] != '+'):
        if int(Data.split()[15]) != 0:
            ask_bid_price_temp = ask_bid_price.copy()
            ask_bid_amt_temp = ask_bid_amt.copy()
        else:
            ask_bid_amt_temp.append(Data.split()[15])
            if int(Data.split()[17]) != 0:
                for i in range(0, 22, 3):
                    ask_bid_amt_temp.append(Data.split()[i + 17])
                ask_bid_amt_temp.append(Data.split()[-7])
                ask_bid_price_temp = [Data.split()[27], Data.split()[28]]
            else:
                ask_bid_amt_temp.append(Data.split()[17])
                if int(Data.split()[19]) != 0:
                    for i in range(0, 16, 3):
                        ask_bid_amt_temp.append(Data.split()[i + 19])
                    ask_bid_amt_temp.append(Data.split()[-9])
                    ask_bid_amt_temp.append(Data.split()[-7])
                    ask_bid_price_temp = [Data.split()[26], Data.split()[27]]
                else:
                    ask_bid_amt_temp.append(Data.split()[19])
                    if int(Data.split()[21]) != 0:
                        for i in range(0, 10, 3):
                            ask_bid_amt_temp.append(Data.split()[i + 21])
                        ask_bid_amt_temp.append(Data.split()[-11])
                        ask_bid_amt_temp.append(Data.split()[-9])
                        ask_bid_amt_temp.append(Data.split()[-7])
                        ask_bid_price_temp = [Data.split()[25], Data.split()[26]]
                    else:
                        ask_bid_amt_temp.append(Data.split()[21])
                        for i in range(0, 4, 3):
                            ask_bid_amt_temp.append(Data.split()[i + 23])
                        ask_bid_amt_temp.append(Data.split()[-13])
                        ask_bid_amt_temp.append(Data.split()[-11])
                        ask_bid_amt_temp.append(Data.split()[-9])
                        ask_bid_amt_temp.append(Data.split()[-7])
                        ask_bid_price_temp = [Data.split()[24], Data.split()[25]]
    else:
        if int(Data.split()[16]) != 0:
            ask_bid_price_temp = ask_bid_price.copy()
            ask_bid_amt_temp = ask_bid_amt.copy()
        else:
            ask_bid_amt_temp.append(Data.split()[16])
            if int(Data.split()[18]) != 0:
                for i in range(0, 22, 3):
                    ask_bid_amt_temp.append(Data.split()[i + 18])
                ask_bid_amt_temp.append(Data.split()[-7])
                ask_bid_price_temp = [Data.split()[28], Data.split()[29]]
            else:
                ask_bid_amt_temp.append(Data.split()[18])
                if int(Data.split()[20]) != 0:
                    for i in range(0, 16, 3):
                        ask_bid_amt_temp.append(Data.split()[i + 20])
                    ask_bid_amt_temp.append(Data.split()[-9])
                    ask_bid_amt_temp.append(Data.split()[-7])
                    ask_bid_price_temp = [Data.split()[27], Data.split()[28]]
                else:
                    ask_bid_amt_temp.append(Data.split()[20])
                    if int(Data.split()[22]) != 0:
                        for i in range(0, 10, 3):
                            ask_bid_amt_temp.append(Data.split()[i + 22])
                        ask_bid_amt_temp.append(Data.split()[-11])
                        ask_bid_amt_temp.append(Data.split()[-9])
                        ask_bid_amt_temp.append(Data.split()[-7])
                        ask_bid_price_temp = [Data.split()[26], Data.split()[27]]
                    else:
                        ask_bid_amt_temp.append(Data.split()[22])
                        for i in range(0, 4, 3):
                            ask_bid_amt_temp.append(Data.split()[i + 24])
                        ask_bid_amt_temp.append(Data.split()[-13])
                        ask_bid_amt_temp.append(Data.split()[-11])
                        ask_bid_amt_temp.append(Data.split()[-9])
                        ask_bid_amt_temp.append(Data.split()[-7])
                        ask_bid_price_temp = [Data.split()[25], Data.split()[26]]

    ask_bid_price = ask_bid_price_temp
    ask_bid_amt = ask_bid_amt_temp

    return ask_bid_price, ask_bid_amt


def append_data(ask_bid_price, ask_bid_amt):

    global df3, df4
    df3 = df3.append(pd.Series(ask_bid_price, index=df3.columns), ignore_index=True)
    df4 = df4.append(pd.Series(ask_bid_amt, index=df4.columns), ignore_index=True)

    return df3, df4


# initialize DataFrame to save the data
global df1, df2, df3, df4
df1 = pd.DataFrame(columns=['Year_Mon_Day', 'Hour_Min_Sec'])
df2 = pd.DataFrame(columns=['KOSPI200', 'USDKRW', 'NextUSD', 'KTB10', 'KTB3'])
df3 = pd.DataFrame(columns=['ask_price', 'bid_price'])
df4 = pd.DataFrame(columns=['ask_5', 'ask_4', 'ask_3', 'ask_2', 'ask_1', 'bid_1', 'bid_2', 'bid_3', 'bid_4', 'bid_5'])


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
today = dt.datetime.now(pytz.timezone('Asia/Seoul')).strftime('%Y%m%d')

# path for saving file
path = 'C:\\Users\\waikorea\\Dropbox\\datafeed\\'

# # Dropbox access token
# import dropbox
# access_token = drpbxtoken
# dbx = dropbox.Dropbox(access_token, timeout=None)

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

    start_t = '090001'  # start receiving data at 9:00
    end_t = '153455'    # end receiving data at 15:35

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
                # concatenate all DFs (date/time + price + ask_bid_price + ask_bid_amt)
                df = pd.concat([df1, df2], axis=1)
                df = pd.concat([df, df3], axis=1)
                df = pd.concat([df, df4], axis=1)
                # wait 1 second for not violating the amount limit for accessing server data
                time.sleep(1)

                if now >= '153500':
                    break

            # save collected data to csv file at given path for every 5 seconds
            df.dropna(subset=['KOSPI200', 'USDKRW', 'NextUSD', 'KTB10', 'KTB3'], how='all', inplace=True)
            df.to_csv(path + today + '.csv', header=True, index=False)
            df.to_csv('C:\\Users\\waikorea\\Desktop\\NH\\CSV\\' + today + '.csv', header=True, index=False)

            # if int(now[4:]) % 20 == 0:
            #     # upload the csv file to dropbox folder
            #     filename = path + today + '.csv'
            #     pathname = "/datafeed/" + today + '.csv'
            #     with open(filename, "rb") as f:
            #         dbx.files_upload(f.read(), pathname, mode=dropbox.files.WriteMode.overwrite)

        # # test to run the program until any specified time
        # if now >= '180001':
        #     break

        # break the while loop at 15:35
        if now >= end_t:
            print('Market closed')
            break

    # # concatenate df1 and df2 (date and time + price)
    # df_full = pd.concat([df1, df2], axis=1)

    # # if there are no data at certain second, fill the previous data
    # df_full.fillna(method='ffill', inplace=True)

    # # drop duplicate rows if there is any
    # df_full.drop_duplicates(subset=['Year/Mon/day', 'Hour:Min:Sec'], inplace=True)

    # # save fully collected data to csv file at given path
    # df_full.to_csv(path + today + '_full.csv', header=True, index=False)

    print('data collection finished')
