# -*- coding: UTF-8 -*-
"""
@author: wycai@pku.edu.cn
"""
from datetime import datetime

import backtrader as bt
import warnings
from tools.date import Date


class DailyBackTestBase(bt.Strategy):
    params = dict(position_adjust_dates=['04-15', '07-15', '10-15', '01-15'], start_date=None, end_date=None)

    def __init__(self):
        self.position_adjust_dates = [Date(date_str=x) for x in self.p.position_adjust_dates]
        self.pre_buylist = []
        self.next_adjust_date = None

    def prenext(self):
        self.log('This trading date is passed and ignored')
        super(DailyBackTestBase, self).prenext()

    def nextstart(self):
        trade_date = Date(self.data.datetime.date().strftime('%Y-%m-%d'))
        self.next_adjust_date = self.get_next_position_adjust_date(trade_date)
        super(DailyBackTestBase, self).nextstart()

    def next(self, *args, **kwargs):
        pass

    def log(self, txt, dt=None):
        """
        Logging function fot this strategy
        """
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def buy(self, data=None, size=None, **kwargs):
        if size % 100 != 0:
            warnings.warn(
                f'Input Size {size} can not be divided by 100, it has been rounded to nearest multiplies of 100.')
            size = round(size / 100) * 100
        if isinstance(data, str):
            data = self.getdatabyname(data)
        if data.close[0] > 1.098 * data.close[-1]:
            self.log('Fail to buy %s due to Buy Trading Limit' % (data._name))
            return None
        else:
            super(DailyBackTestBase, self).buy(data=data, size=size, exectype=bt.Order.Close, **kwargs)

    def sell(self, data=None, size=None, **kwargs):
        if size % 100 != 0:
            warnings.warn(
                f'Input Size {size} can not be divided by 100, it has been rounded to nearest multiplies of 100.')
            size = round(size / 100) * 100
        if isinstance(data, str):
            data = self.getdatabyname(data)
        if data.close[0] < 0.902 * data.close[-1]:
            self.log('Fail to buy %s due to Sell Trading Limit' % (data._name))
            return None
        else:
            super(DailyBackTestBase, self).sell(data=data, size=size, exectype=bt.Order.Close, **kwargs)

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enougth cash
        if order.status in [order.Completed, order.Canceled, order.Margin]:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, Stock: %s, Price: %.2f, Cost: %.2f, Comm %.2f' %
                    (order.params.data._name,
                     order.executed.price,
                     order.executed.value,
                     order.executed.comm))
            else:  # Sell
                self.log('SELL EXECUTED, Stock: %s, Price: %.2f, Cost: %.2f, Comm %.2f' %
                         (order.params.data._name,
                          order.executed.price,
                          order.executed.value,
                          order.executed.comm))

    def notify_trade(self, trade):
        if trade.isclosed:
            self.log('TRADE PROFIT, GROSS %.2f, NET %.2f' %
                     (trade.pnl, trade.pnlcomm))

    def get_next_position_adjust_date(self, date: Date):
        tmp = [x for x in self.position_adjust_dates if x > date]
        next_adjust_dayint = min(tmp) if tmp else min(self.position_adjust_dates)
        return next_adjust_dayint