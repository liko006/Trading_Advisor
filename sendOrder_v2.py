import sys
import time
import pytz
import datetime as dt
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import pymysql
from PyQt5.QtWidgets import *
from PyQt5.QAxContainer import *
from PyQt5.QtCore import *
import logging

pymysql.install_as_MySQLdb()

logger = logging.getLogger('trace')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# log 출력
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# log를 파일에 출력
file_handler = logging.FileHandler('order.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

logger2 = logging.getLogger('trace2')
logger2.setLevel(logging.INFO)
formatter2 = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# log2를 파일에 출력
file_handler2 = logging.FileHandler('sql_reading.log')
file_handler2.setFormatter(formatter2)
logger2.addHandler(file_handler2)

logger3 = logging.getLogger('trace3')
logger3.setLevel(logging.INFO)
formatter3 = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# log3를 파일에 출력
file_handler3 = logging.FileHandler('signal.log')
file_handler3.setFormatter(formatter3)
logger3.addHandler(file_handler3)

# read nh pwd and db pwd from text file
with open('./nh_r_info.txt', 'r') as f:
    lines = f.readlines()
nhpwd = lines[0]
certipwd = lines[1]
actpwd = lines[2]

with open('./db_info.txt', 'r') as f:
    lines = f.readlines()
dbpwd = lines[0]


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
        self.dynamicCall("OLogin(QString, QString, QString)", 'limpst', '1230', '')
        self.login_event_loop = QEventLoop()
        self.login_event_loop.exec_()

    def login_complete(self, code):
        if code == 1:
            print('login completed')
        else:
            print('login failed')
        self.login_event_loop.exit()

    def send_order(self, ticker, signal):
        self.dynamicCall("OSendNewOrder(QString, QString, QString, int, int, int, int, float, int)",
                                        ['107470', '1230', ticker, signal, 2, 3, 1, None, 1])
        logger.info(f'{signal} order for {ticker} sent to server')
        # 계좌번호, 계좌비밀번호, 종목, 매수/매도 구분, 가격구분, 체결구분, 주문수량, 주문가격, 고객지정번호
        # 시장가 주문시, 주문가격은 argument 에서 제거

        self.sendorder_event_loop = QEventLoop()
        self.sendorder_event_loop.exec_()

    def recv_data(self, DataType, TrCode, RqID, DataSize, Data):
        logger.info(f'order respond from server : {DataType}, {TrCode}, {RqID}, {DataSize}, {Data}')
        # print('order completed')

        self.sendorder_event_loop.exit()


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


def prep_signal_df(df, now):
    # sort output data by time
    output_data = df.sort_values("CRE_DATE")

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
    global df_signal
    df_signal = df_signal.append(pd.Series([now, kospi_signal, usd_signal], index=df_signal.columns),
                                 ignore_index=True)
    return df_signal


def trade_type_fol(col, prev_signal, latest_signal, tickers, nh, num_trade):
    # check previous and latest signal
    if prev_signal[col].item() == latest_signal[col].item():
        pass
    else:
        if latest_signal[col].item() == 1:  # buy signal
            ticker = tickers[col.split('_')[0]]
            orderType = 1  # buy order type
            nh.send_order(ticker, orderType)  # send buy order
            num_trade += 1
        elif latest_signal[col].item() == -1:  # sell signal
            ticker = tickers[col.split('_')[0]]
            orderType = 2  # sell order type
            nh.send_order(ticker, orderType)  # send sell order
            num_trade += 1
        else:
            pass
    return num_trade


def trade_type_ops(col, prev_signal, latest_signal, tickers, nh, num_trade):
    # check previous and latest signal
    if prev_signal[col].item() == latest_signal[col].item():
        pass
    else:
        if latest_signal[col].item() == 1:  # buy signal
            ticker = tickers[col.split('_')[0]]
            orderType = 2  # sell order type
            nh.send_order(ticker, orderType)  # send sell order
            num_trade += 1
        elif latest_signal[col].item() == -1:  # sell signal
            ticker = tickers[col.split('_')[0]]
            orderType = 1  # buy order type
            nh.send_order(ticker, orderType)  # send buy order
            num_trade += 1
        else:
            pass
    return num_trade


# 종목 Code : 코스피200 지수 9월 만기 (101R9000)
# 종목 Code : USD/KRW 통화 7월 만기 (175R7000) / 175년월000 (년 - R:2021, S:2022 / 월 2~9는 숫자 10,11,12는 A,B,C)
# 종목 Code : 국고채 10년물 최종거래일 6월 (167R6000)
# kospi = '101R9000'
# usd = '175R7000'
# nextusd = '175R8000'
# ktb10 = '167R9000'
# ktb3 = '165R9000'
tickers = {'kospi': '101R9000', 'usd': '175R7000', 'nextusd': '175R8000', 'ktb10': '167R9000', 'ktb3': '165R9000'}
today = dt.datetime.now(pytz.timezone('Asia/Seoul')).strftime('%Y%m%d')

# create engine to connect to DB server
engine = create_engine("mysql+mysqldb://limpst:" + dbpwd + "@119.205.211.179/waikorea_port", encoding='utf-8')

# create DataFrame for storing signal data
global df_signal
df_signal= pd.DataFrame(columns=['Time', 'kospi_Signal', 'usd_Signal'])

# start the automated trading program
if __name__ == "__main__":

    # create QApp object
    app = QApplication(sys.argv)

    # create QAxWidget object
    nh = NH()

    # connect to the server
    nh.connect_server()

    # log-in
    nh.login()

    start_t = '090301'  # start at 9:03
    end_t = '153401'  # end at 15:34

    # number of transactions
    num_trade = 0

    while True:

        now = dt.datetime.now(pytz.timezone('Asia/Seoul')).strftime('%H%M%S')

        # load analyzed data and call signal from it
        signal = 0

        if now < start_t:
            continue

        # start the process at start time
        if now >= start_t:

            if int(now[4:]) % 6 == 0:

                # read sql table data and save it into DataFrame
                output_data = pd.read_sql("output_data", con=engine, index_col='SEQ', parse_dates='CRE_DATE')
                logger2.info(f'read data from sql : {output_data.sort_values("CRE_DATE").dropna(how="all").iloc[-6:, :3]}')
                # preprocess df_signal
                prep_df_signal = prep_signal_df(output_data, now)
                # get latest signal
                latest_signal = prep_df_signal.tail(1)
                # get previous signal
                prev_signal = prep_df_signal.tail(2).drop(latest_signal.index)
                logger3.info(f'signal data : {prep_df_signal}')

                # check for signals
                # need to check each ticker has 0 signal or not
                if (latest_signal.iloc[0, 1]) == 0 and (latest_signal.iloc[0, 2]) == 0:
                    pass
                else:
                    for col in latest_signal.columns.tolist()[1:]:
                        if col.split('_')[0] == 'kospi':
                            num_trade = trade_type_fol(col, prev_signal, latest_signal, tickers, nh, num_trade)
                        elif col.split('_')[0] == 'usd':
                            num_trade = trade_type_ops(col, prev_signal, latest_signal, tickers, nh, num_trade)
                        time.sleep(1)

            # check total number of trades
            if int(now[2:]) == 0:
                print(num_trade)

        # # test to run the program until any specified time
        # if now >= '165900':
        #     break

        # break the while loop at end time
        if now >= end_t:
            print('Market Closed')
            break
