# -*- coding: UTF-8 -*-
"""
@author: wycai@pku.edu.cn
"""
import backtrader as bt
import pandas as pd
import numpy as np

__all__ = ['IC', 'Rank_IC']


class IC(bt.Analyzer):
    '''This analyzer calculates mid & long term strategy's information coefficient,
    calculated as the correlation between the ranking for stocks on last rebalance
    and current earning ranking

    Methods:

      - ``get_analysis``

        Returns a dictionary (with . notation support and subdctionaries) with
        drawdown stats as values, the following keys/attributes are available:

        - ``IC`` - IC value in 0.xx %
        - ``IR`` - Information Ratio in 0.xx % = avg(IC)/std(IC)
        - ``len`` - drawdown length
    '''
    params = dict(Rank_IC=False)

    def create_analysis(self):
        self.rets = bt.AutoOrderedDict()

    def nextstart(self):
        self.strategy.rank_dict = dict()

    def stop(self):
        if not self.strategy.rank_dict:
            self.rets['Error'] = 'Your forget to add rank_dict in your strategy!'
        else:
            rank_dict = self.strategy.rank_dict

        last_i: int = -1
        last_dt: str = ''
        last_rank: list = []  # position rank
        ret = dict()  # cur ret for last position
        for i in range(len(self.data) - 1, -1, -1):
            cur_dt = self.data.datetime.date(-i).strftime('%Y-%m-%d')
            if cur_dt not in rank_dict.keys() and i != 0:
                continue
            if last_i != -1:
                for stock in last_rank:
                    stock_data = self.strategy.getdatabyname(stock)
                    ret[stock] = stock_data.close[-i] / stock_data.close[-last_i] - 1
                cur_rank = sorted(ret.keys(), key=lambda x: ret[x], reverse=True)
                if self.p.Rank_IC:
                    self.rets[last_dt] = pd.Series([cur_rank.index(key) for key in last_rank]).corr(
                        pd.Series(list(range(len(last_rank)))), method='spearman')
                else:
                    self.rets[last_dt] = pd.Series([cur_rank.index(key) for key in last_rank]).corr(
                        pd.Series(list(range(len(last_rank)))), method='pearson')
            if i != 0:
                last_i = i
                last_dt = cur_dt
                last_rank = rank_dict[cur_dt]

        ICs = list(self.rets.values())
        self.rets.final.IC_avg = np.mean(ICs)
        self.rets.final.IC_std = np.std(ICs)
        self.rets.final.IR = self.rets.final['IC_avg'] / self.rets.final['IC_std']


class Rank_IC(IC):
    params = dict(Rank_IC=True)