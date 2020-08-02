# -*- coding: utf-8 -*- 
"""
author: Kyle Cai
e-mail: wycai@pku.edu.cn
"""

#%%
import pandas as pd
from math import ceil

import backtrader as bt
from strategy.strategy_base import DailyBackTestBase
from datas.database import DataBase

#%%
class IncomeData(bt.feeds.PandasData):
    lines = ('NETPROFIT_TTM2',)
    params = (('NETPROFIT_TTM2',-1),)

class PTStrategy(DailyBackTestBase):
    def __init__(self):
        self.pre_buylist = []

    def next(self):
        trade_date = self.data0.datetime.date().strftime('%Y-%m-%d')
        if trade_date[5:] in ['04-15', '07-15', '10-15', '01-15']:
            stocklist = index_component.loc[index_component.index==trade_date, 'stocklist'].values[0]
            profit = {key:self.getdatabyname(key).NETPROFIT_TTM2[0] for key in stocklist}
            buylist = sorted(profit.keys(), key = lambda x: profit[x], reverse=True)[:10]

            value = self.broker.get_value()/10
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


# %%
if __name__ == '__main__':
#%%
    # load data
    trading_data = DataBase(table_name = 'daily_trading', db_name = 'stock').from_mysql('*')
    trading_data['tradedate'] = pd.to_datetime(trading_data['tradedate'])
    trading_data['openinterest'] = 0
    income_data = DataBase(table_name = 'income_sheet', db_name = 'stock').from_mysql('NETPROFIT_TTM2, tradecode, tradedate')
    income_data['tradedate'] = pd.to_datetime(income_data['tradedate'])
    trading_data = trading_data.merge(income_data, on=['tradecode', 'tradedate'], how='left')
    trading_data_groups = trading_data.groupby(by='tradecode')
    index_component = DataBase(table_name = 'daily_component', db_name = 'index_related').from_mysql('*')[['date', 'wind_code']]
    index_component['tradedate'] = pd.to_datetime(index_component['date'])
    index_component = pd.DataFrame(index_component.groupby(by='tradedate').apply(lambda x: x['wind_code'].values), columns=['stocklist'])

    cerebro = bt.Cerebro(stdstats=False)
    # add a strategy
    cerebro.addstrategy(PTStrategy)
    # add data
    for windcode, data in trading_data_groups:
        data = data.sort_values(by='tradedate').fillna(method='ffill')
        data = data.set_index('tradedate').drop('tradecode', axis=1)
        cerebro.adddata(IncomeData(dataname=data, plot=False), name=windcode)
        cerebro.datas[-1].csv = False
    cerebro.broker.setcommission(commission=0.0005)
    cerebro.broker.setcash(1000000.0)

    cerebro.addwriter(bt.WriterFile, out='result.csv', csv=True)
    cerebro.addobserver(bt.observers.Broker)
    cerebro.observers[0][1].csv= True
    cerebro.addobserver(bt.observers.Trades)
    cerebro.observers[1][1].csv = False
    cerebro.addobserver(bt.observers.BuySell)
    cerebro.observers[2][1].csv = False

    # add indicators
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='_AnnualReturn')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='_DrawDown')
    cerebro.addanalyzer(bt.analyzers.TimeDrawDown, _name='_TimeDrawDown')
    cerebro.addanalyzer(bt.analyzers.PeriodStats, _name='_PeriodStats')
    cerebro.addanalyzer(bt.analyzers.Returns, _name='_Returns')
    cerebro.addanalyzer(bt.analyzers.SharpeRatio_A, _name='_SharpeRatio_A')
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='_TimeReturn')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='_TradeAnalyzer')
    cerebro.addanalyzer(bt.analyzers.Transactions, _name='_Transactions')
    results = cerebro.run()

    # get pnl
    portvalue = cerebro.broker.getvalue()
    pnl = portvalue - 1000000

    print(f'total return: {round(pnl, 2)}')
    print(f'total valuie: {round(portvalue, 2)}')

#%%
    plot = cerebro.plot(style='candlestick')[0][0]
    plot.savefig('plot.pdf', bbox_inches='tight')