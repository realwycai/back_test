# -*- coding: utf-8 -*- 
"""
author: Kyle Cai
e-mail: wycai@pku.edu.cn
"""

# %%
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
    params = dict(stock_num=6, start_date=None, end_date=None)

    def __init__(self):
        self.pre_buylist = []
        self.rank_dict = dict()
        self.next_adjust_date = None
        super(PTStrategy, self).__init__()

    def nextstart(self):
        trade_date = Date(self.data.datetime.date().strftime('%Y-%m-%d'))
        self.next_adjust_date = self.get_next_position_adjust_date(trade_date)
        super(DailyBackTestBase, self).nextstart()

    def next(self):
        trade_date = Date(self.data.datetime.date().strftime('%Y-%m-%d'))
        if trade_date >= self.next_adjust_date and \
                (self.p.start_date is None or (trade_date >= Date(self.p.start_date))) and \
                (self.p.end_date is None or (trade_date <= Date(self.p.end_date))):
            stocklist = index_component.loc[index_component.index == trade_date.to_str(), 'stocklist'].values[0]
            profit_velocity = {key: self.getdatabyname(key).profit_velocity[0] for key in stocklist}
            score = {key: 0 for key in stocklist}
            rank_1 = sorted(profit_velocity.keys(), key=lambda x: profit_velocity[x], reverse=True)
            rank_11 = rank_1[:int(len(rank_1) / 3)]
            rank_12 = rank_1[int(len(rank_1) / 3):int(2 * len(rank_1) / 3)]
            rank_13 = rank_1[int(2 * len(rank_1) / 3):]
            profit_acceleration_11 = {key: self.getdatabyname(key).profit_acceleration[0] for key in rank_11}
            profit_acceleration_mean_11 = np.mean(list(profit_acceleration_11.values()))
            profit_acceleration_std_11 = np.std(list(profit_acceleration_11.values()))
            profit_acceleration_12 = {key: self.getdatabyname(key).profit_acceleration[0] for key in rank_12}
            profit_acceleration_mean_12 = np.mean(list(profit_acceleration_12.values()))
            profit_acceleration_std_12 = np.std(list(profit_acceleration_12.values()))
            profit_acceleration_13 = {key: self.getdatabyname(key).profit_acceleration[0] for key in rank_13}
            profit_acceleration_mean_13 = np.mean(list(profit_acceleration_13.values()))
            profit_acceleration_std_13 = np.std(list(profit_acceleration_13.values()))
            for key in rank_11:
                score[key] += 3 + (
                            profit_acceleration_11[key] - profit_acceleration_mean_11) / profit_acceleration_std_11
            for key in rank_12:
                score[key] += 2 + (
                            profit_acceleration_12[key] - profit_acceleration_mean_12) / profit_acceleration_std_12
            for key in rank_13:
                score[key] += 1 + (
                            profit_acceleration_13[key] - profit_acceleration_mean_13) / profit_acceleration_std_13
            buylist = sorted(score.keys(), key=lambda x: score[x], reverse=True)[:self.p.stock_num]
            self.rank_dict[trade_date.to_str()] = sorted(score.keys(), key=lambda x: score[x], reverse=True)

            value = self.broker.get_value() / self.p.stock_num
            for stock_code in self.pre_buylist:
                stock = self.getdatabyname(stock_code)
                current_value = self.broker.get_value([stock])
                if stock_code not in buylist:
                    self.sell(stock, size=self.positions[stock].size)
                elif current_value > value:
                    sell_size = ceil((current_value - value) / stock.close[0] / 100) * 100
                    self.sell(stock, size=sell_size)
            for stock_code in self.pre_buylist:
                stock = self.getdatabyname(stock_code)
                current_value = self.broker.get_value([stock])
                if current_value < value and stock_code in buylist:
                    buy_size = int((value - current_value) / stock.close[0] / 100) * 100
                    self.buy(stock, size=buy_size)
            for stock_code in buylist:
                if stock_code not in self.pre_buylist:
                    stock = self.getdatabyname(stock_code)
                    buy_size = int(value / stock.close[0] / 100) * 100
                    self.buy(stock, size=buy_size)
            self.pre_buylist = buylist
            self.log('total portfolio value: ' + str(self.broker.get_value()))
        else:
            # self.log('current time skip')
            pass
        self.next_adjust_date = self.get_next_position_adjust_date(trade_date)


if __name__ == '__main__':
    # %%
    # load data
    trading_data = DataBase(table_name='daily_trading', db_name='stock').from_mysql('*')
    trading_data['tradedate'] = pd.to_datetime(trading_data['tradedate'])
    trading_data['openinterest'] = 0
    bench_data = trading_data[trading_data.tradecode == '000016.SH'].reset_index(drop=True)
    trading_data = trading_data[trading_data.tradecode != '000016.SH'].reset_index(drop=True)
    income_data = DataBase(table_name='income_sheet', db_name='stock').from_mysql(
        'NETPROFIT_TTM2, QFA_NP_BELONGTO_PARCOMSH, tradecode, tradedate')
    income_data['tradedate'] = pd.to_datetime(income_data['tradedate'])
    income_data['profit_acceleration'] = None
    income_data_group = income_data.sort_values(by=['tradecode', 'tradedate']).groupby('tradecode')
    income_data = pd.DataFrame()
    for tradecode, data in income_data_group:
        data = data.reset_index(drop=True)
        data['profit_velocity'] = data.NETPROFIT_TTM2 / data.NETPROFIT_TTM2.shift(1)
        count = 0
        for j in range(len(data)):
            if pd.isna(data.loc[j, 'QFA_NP_BELONGTO_PARCOMSH']):
                continue
            elif count < 5:
                count += 1
                continue
            else:
                X = np.arange(5).reshape(-1, 1)
                X = np.hstack([X ** 2, X])
                X = np.column_stack([[1] * 5, X])
                beta = np.dot(np.linalg.inv(np.dot(X.T, X)),
                              np.dot(X.T, data.loc[j - 5:j - 1, 'QFA_NP_BELONGTO_PARCOMSH'].values))
                data.loc[j, 'profit_acceleration'] = beta[1]
        income_data = income_data.append(data[['tradecode', 'tradedate', 'profit_velocity', 'profit_acceleration']],
                                         ignore_index=True)
    trading_data = trading_data.merge(income_data, on=['tradecode', 'tradedate'], how='left')
    trading_data_groups = trading_data.groupby(by='tradecode')
    index_component = DataBase(table_name='daily_component', db_name='index_related').from_mysql('*')[
        ['date', 'wind_code']]
    index_component['tradedate'] = pd.to_datetime(index_component['date'])
    index_component = pd.DataFrame(index_component.groupby(by='tradedate').apply(lambda x: x['wind_code'].values),
                                   columns=['stocklist'])

    cerebro = bt.Cerebro(stdstats=False)
    # add a strategy
    cerebro.addstrategy(PTStrategy, position_adjust_dates=['04-30', '07-15', '10-15', '01-31'], stock_num=6, start_date='2012-01-01', end_date='2020-01-01')
    # add data
    for windcode, data in trading_data_groups:
        data = data.sort_values(by='tradedate').fillna(method='ffill').fillna(-999999)
        data = data.set_index('tradedate').drop('tradecode', axis=1)
        cerebro.adddata(IncomeData(dataname=data, plot=False), name=windcode)
        cerebro.datas[-1].csv = False
    bench_data = bench_data.set_index('tradedate').drop('tradecode', axis=1)
    bench_data_feeds = bt.feeds.PandasData(dataname=bench_data)
    cerebro.adddata(bench_data_feeds, name='000016.SH')
    cerebro.broker.setcommission(commission=0.0005)
    cerebro.broker.setcash(1000000.0)

    cerebro.addwriter(bt.WriterFile, out='result.csv', csv=True)
    cerebro.addobserver(bt.observers.Broker)
    cerebro.observers[-1][1].csv = True
    cerebro.addobserver(bt.observers.Trades)
    cerebro.observers[-1][1].csv = False
    cerebro.addobserver(bt.observers.BuySell)
    cerebro.observers[-1][1].csv = False
    cerebro.addobserver(bt.observers.Benchmark, data=bench_data_feeds, timeframe=bt.TimeFrame.NoTimeFrame)
    cerebro.observers[-1][1].csv = True
    cerebro.addobserver(bt.observers.DrawDown)
    cerebro.observers[-1][1].csv = False

    # add indicators
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='_AnnualReturn')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='_DrawDown')
    cerebro.addanalyzer(bt.analyzers.PeriodStats, _name='_PeriodStats')
    cerebro.addanalyzer(bt.analyzers.SharpeRatio_A, _name='_SharpeRatio_A')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='_TradeAnalyzer')
    cerebro.addanalyzer(bt.analyzers.Rank_IC, _name='_Rank_IC')
    results = cerebro.run()

    # get pnl
    portvalue = cerebro.broker.getvalue()
    pnl = portvalue - 1000000

    print(f'total return: {round(pnl, 2)}')
    print(f'total valuie: {round(portvalue, 2)}')

    # %%
    plot = cerebro.plot(style='candlestick')[0][0]
    plot.savefig('plot.pdf', bbox_inches='tight')
