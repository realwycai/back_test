# -*- coding: utf-8 -*- 
"""
author: Kyle Cai
e-mail: wycai@pku.edu.cn
"""

#%%
import pandas as pd
import numpy as np
from math import ceil

import backtrader as bt
from strategy.strategy_base import DailyBackTestBase
from datas.database import DataBase
from tools.date import Date


class IncomeData(bt.feeds.PandasData):
    lines = ('profit_velocity', 'profit_acceleration',)
    params = (('profit_velocity', 6),
              ('profit_acceleration', 7),
              )

class PTStrategy(DailyBackTestBase):
    def __init__(self):
        self.pre_buylist = []
        self.next_adjust_date = None
        super(PTStrategy, self).__init__()

    def nextstart(self):
        trade_date = Date(self.data0.datetime.date().strftime('%Y-%m-%d'))
        self.next_adjust_date = self.next_position_adjust_date(trade_date)
        super(DailyBackTestBase, self).nextstart()

    def next(self):
        trade_date = Date(self.data0.datetime.date().strftime('%Y-%m-%d'))
        if trade_date >= self.next_adjust_date:
            stocklist = index_component.loc[index_component.index==trade_date.to_str(), 'stocklist'].values[0]
            profit_velocity = {key:self.getdatabyname(key).profit_velocity[0] for key in stocklist}
            buylist = sorted(profit_velocity.keys(), key = lambda x: profit_velocity[x], reverse=True)[:18]
            profit_acceleration = {key:self.getdatabyname(key).profit_acceleration[0] for key in buylist}
            buylist = sorted(profit_acceleration.keys(), key = lambda x: profit_acceleration[x], reverse=True)[:6]

            value = self.broker.get_value()/6
            for stock_code in self.pre_buylist:
                stock = self.getdatabyname(stock_code)
                current_value = self.broker.get_value([stock])
                if stock_code not in buylist:
                    self.sell(stock, size=self.positions[stock].size)
                elif current_value > value:
                    sell_size = ceil((current_value-value)/stock.close[0]/100)*100
                    self.sell(stock, size=sell_size)
            for stock_code in self.pre_buylist:
                stock = self.getdatabyname(stock_code)
                current_value = self.broker.get_value([stock])
                if current_value < value and stock_code in buylist:
                    buy_size = int((value-current_value)/stock.close[0]/100)*100
                    self.buy(stock, size=buy_size)
            for stock_code in buylist:
                if stock_code not in self.pre_buylist:
                    stock = self.getdatabyname(stock_code)
                    buy_size = int(value / stock.close[0] / 100) * 100
                    self.buy(stock, size=buy_size)
            self.pre_buylist = buylist
            self.log('total portfolio value: '+str(self.broker.get_value()))
        else:
            # self.log('current time skip')
            pass
        self.next_adjust_date = self.next_position_adjust_date(trade_date)

# %%
if __name__ == '__main__':
#%%
    # load data
    trading_data = DataBase(table_name = 'daily_trading', db_name = 'stock').from_mysql('*')
    trading_data['tradedate'] = pd.to_datetime(trading_data['tradedate'])
    trading_data['openinterest'] = 0
    income_data = DataBase(table_name = 'income_sheet', db_name = 'stock').from_mysql('NETPROFIT_TTM2, QFA_NP_BELONGTO_PARCOMSH, tradecode, tradedate')
    income_data['tradedate'] = pd.to_datetime(income_data['tradedate'])
    income_data['profit_acceleration'] = None
    income_data_group = income_data.sort_values(by=['tradecode', 'tradedate']).groupby('tradecode')
    income_data = pd.DataFrame()
    for tradecode, data in income_data_group:
        data = data.reset_index(drop=True)
        data['profit_velocity'] = data.NETPROFIT_TTM2/data.NETPROFIT_TTM2.shift(1)
        count = 0
        for j in range(len(data)):
            if pd.isna(data.loc[j,'QFA_NP_BELONGTO_PARCOMSH']):
                continue
            elif count<5:
                count += 1
                continue
            else:
                X = np.arange(5).reshape(-1,1)
                X = np.hstack([X**2, X])
                X = np.column_stack([[1]*5, X])
                beta = np.dot(np.linalg.inv(np.dot(X.T,X)), np.dot(X.T, data.loc[j-5:j-1,'QFA_NP_BELONGTO_PARCOMSH'].values))
                data.loc[j,'profit_acceleration'] = beta[1]
        income_data = income_data.append(data[['tradecode', 'tradedate', 'profit_velocity', 'profit_acceleration']], ignore_index=True)
    trading_data = trading_data.merge(income_data, on=['tradecode', 'tradedate'], how='left')
    trading_data_groups = trading_data.groupby(by='tradecode')
    index_component = DataBase(table_name = 'daily_component', db_name = 'index_related').from_mysql('*')[['date', 'wind_code']]
    index_component['tradedate'] = pd.to_datetime(index_component['date'])
    index_component = pd.DataFrame(index_component.groupby(by='tradedate').apply(lambda x: x['wind_code'].values), columns=['stocklist'])

    cerebro = bt.Cerebro(stdstats=False)
    # add a strategy
    cerebro.addstrategy(PTStrategy, position_adjust_dates=['04-30', '07-15', '10-15', '01-31'])
    # add data
    for windcode, data in trading_data_groups:
        data = data.sort_values(by='tradedate').fillna(method='ffill').fillna(-999999)
        data = data.set_index('tradedate').drop('tradecode', axis=1)
        cerebro.adddata(IncomeData(dataname=data, plot=False), name=windcode)
        cerebro.datas[-1].csv = False
    cerebro.broker.setcommission(commission=0.0005)
    cerebro.broker.setcash(1000000.0)

    cerebro.addwriter(bt.WriterFile, out='result.csv', csv=True)
    cerebro.addobserver(bt.observers.Broker)
    cerebro.observers[-1][1].csv= True
    cerebro.addobserver(bt.observers.Trades)
    cerebro.observers[-1][1].csv = False
    cerebro.addobserver(bt.observers.BuySell)
    cerebro.observers[-1][1].csv = False
    # cerebro.addobserver(bt.observers.Benchmark, data=benchdata, timeframe=TIMEFRAMES[args.timeframe])
    cerebro.addobserver(bt.observers.DrawDown)
    cerebro.observers[-1][1].csv = False

    # add indicators
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='_AnnualReturn')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='_DrawDown')
    cerebro.addanalyzer(bt.analyzers.PeriodStats, _name='_PeriodStats')
    cerebro.addanalyzer(bt.analyzers.SharpeRatio_A, _name='_SharpeRatio_A')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='_TradeAnalyzer')
    results = cerebro.run()

    # get pnl
    portvalue = cerebro.broker.getvalue()
    pnl = portvalue - 1000000

    print(f'total return: {round(pnl, 2)}')
    print(f'total valuie: {round(portvalue, 2)}')

#%%
    plot = cerebro.plot(style='candlestick')[0][0]
    plot.savefig('plot.pdf', bbox_inches='tight')