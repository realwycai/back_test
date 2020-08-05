# -*- coding: utf-8 -*- 
"""
author: Kyle Cai
e-mail: wycai@pku.edu.cn
"""
import sys
from os.path import dirname
sys.path.append(dirname(dirname(dirname(__file__))))

from datas.data_type import DataType
from datas.database import WSETData, WSDData, DataBase
from WindPy import w

def fetch_index_change():
    index_io = WSETData(data_type = "indexhistory",
                        field = "startdate=2010-06-26;enddate=2020-07-26;windcode=000016.SH;field=tradedate,tradecode,tradename,tradestatus",
                        table_name = 'component_change',
                        db_name = 'index_related',
                       )
    index_io.fetch_data()
    index_io.to_mysql()


def fetch_daily_trading_data():
    with open('sz50_stock_list.txt', 'r') as f:
        sz50 = f.read().split()
    data_list = []
    i = 0
    for stock in sz50:
        data_list.append(WSDData(data_type = DataType.daily_trading,
                         code = stock,
                         field = "open,high,low,close,volume",
                         table_name = 'daily_trading',
                         db_name = 'stock',
                         start = '2010-06-26',
                         end = '2020-07-26')
                         )
        data_list[-1].fetch_data()
        data_list[-1].to_mysql()
        print(f'{i}th {stock} finished')
        i += 1

def fetch_daily_index_data():
    index_data = WSDData(data_type = DataType.daily_trading,
                         code = '000016.SH',
                         field = "open,high,low,close,volume",
                         table_name = 'daily_trading',
                         db_name = 'stock',
                         start = '2010-06-26',
                         end = '2020-07-26'
                         )
    index_data.fetch_data()
    index_data.to_mysql()


def fetch_income_sheet_data():
    with open('sz50_stock_list.txt', 'r') as f:
        sz50 = f.read().split()
    data_list = []
    i = 0
    for stock in sz50:
        data_list.append(WSDData(data_type = DataType.financial_report,
                         code = stock,
                         field = "np_belongto_parcomsh,net_profit_is,qfa_np_belongto_parcomsh,qfa_net_profit_is,netprofit_ttm2,profit_ttm2",
                         table_name = 'income_sheet',
                         db_name = 'stock',
                         start = '2010-06-26',
                         end = '2020-07-26')
                         )
        data_list[-1].fetch_data()
        data_list[-1].to_mysql()
        print(f'{i}th {stock} finished')
        i += 1


def fetch_index_component():
    trading_data = DataBase(table_name='daily_trading', db_name='stock').from_mysql('*')
    trading_dates = trading_data['tradedate'].drop_duplicates().sort_values()
    data_list = []
    for trading_date in trading_dates:
        data_list.append(WSETData(
                         data_type = DataType.index_component,
                         field = f"date={trading_date.strftime('%Y-%m-%d')};sectorid=a00103010b000000",
                         table_name = 'daily_component',
                         db_name = 'index_related'
                         ))
        data_list[-1].fetch_data()
        data_list[-1].to_mysql()
        print(f'{trading_date} finished')


if __name__ == '__main__':
    w.start()
    # # SZ50 component change
    # fetch_index_change()
    # # SZ50 component daily trading data
    # fetch_daily_trading_data()
    # # SZ50 component income sheet data
    # fetch_income_sheet_data()
    # # SZ daily component
    # fetch_index_component()
    # # SZ50 dailydata
    fetch_daily_index_data()
    w.close()