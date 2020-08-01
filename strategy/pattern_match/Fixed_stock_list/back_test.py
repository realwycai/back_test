import os
import pandas as pd
import psycopg2
import numpy as np
import time
import Fixed_stock_list.CORN_K as CORN_K
import Fixed_stock_list.Symmetric_CORN_K as Symmetric_CORN_K
import Fixed_stock_list.Functional_CORN_K as Functional_CORN_K

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


# 以上证50 000016.SH 在2014年初20140102的为dataset，以20120104为初始的相关系数考虑日期。

def main():
    W = [1, 2, 3, 4, 5]
    Pho = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6]
    Lambda = [10, 100, 500]
    k = 5
    # 选择股票范围
    MYDB = psycopg2.connect(database="MYDB", user="postgres", password="pgread", host="localhost", port="5432")
    sql = """SELECT * FROM china_index_component WHERE s_info_windcode='000016.SH'"""
    index_component = pd.read_sql(sql, MYDB)
    index_component.s_con_outdate = index_component.s_con_outdate.fillna('')
    index_component = index_component.loc[(index_component.s_con_indate <= '20140102') & (
            (index_component.s_con_outdate >= '20140102') | (index_component.s_con_outdate == '')), :]
    index_component = index_component[~(index_component.s_con_windcode == '601299.SH')]  # '601299.SH'于2015年5月6日退市
    FACTORDB = psycopg2.connect(database="FACTORDB", user="postgres", password="pgread", host="localhost", port="5432")
    sql = """SELECT * FROM factor_size WHERE trade_dt='20110104'"""
    size = pd.read_sql(sql, FACTORDB)
    index_component = index_component.merge(size, left_on='s_con_windcode', right_on='s_info_windcode')
    index_component = index_component.sort_values('sizefmv', ascending=False).reset_index(drop=True).loc[0:15,:]
    sql = """SELECT s_info_windcode, trade_dt, s_dq_adjclose FROM china_stock_tradesdaily WHERE trade_dt>='20111220'"""
    price_daily = pd.read_sql(sql, MYDB)
    price_daily = price_daily[price_daily.s_info_windcode.isin(index_component.s_con_windcode)]
    # 计算behavior
    price_daily = price_daily.groupby(by='s_info_windcode').apply(lambda x: x.sort_values('trade_dt', ascending=True))
    price_daily = price_daily.set_index('trade_dt', drop=True)
    price_daily = price_daily.groupby(by='s_info_windcode').apply(
        lambda x: x.s_dq_adjclose / x.s_dq_adjclose.shift(1)).reset_index(
        drop=False)
    # price_daily = price_daily.sort_values(by=['trade_dt', 's_info_windcode']).rename(columns={'s_dq_adjclose': 'ratio_dq_price'})
    price_daily = price_daily.melt(id_vars=['s_info_windcode'], value_name='ratio_dq_price')
    price_daily = price_daily.sort_values(by=['trade_dt', 's_info_windcode'])
    trade_dt = price_daily.trade_dt.drop_duplicates().sort_values(ascending=True).reset_index(drop=True)
    # 计算相关系数矩阵
    price_daily_wide = price_daily.pivot(index='trade_dt', columns='s_info_windcode', values='ratio_dq_price')
    col_name = list(trade_dt[trade_dt >= '20120104'])
    col_name.insert(0, 'w')
    col_name.insert(1, 'trade_dt')
    correlation = pd.DataFrame(columns=col_name)
    for w in W:
        temp = price_daily_wide.shift(1)
        for i in range(1, w):
            temp = pd.concat([temp, price_daily_wide.shift(i + 1)], axis=1)
        coefficient = temp[temp.index >= '20120104'].T.corr()
        coefficient['w'] = w
        coefficient = coefficient.reset_index(drop=False)
        correlation = correlation.append(coefficient, ignore_index=True, sort=True)
    s = 10000
    portfolio = pd.DataFrame(columns=['trade_dt', 'portfolio', 'RET'])
    start = time.time()
    for i in trade_dt[trade_dt[trade_dt == '20140102'].index.values[0]:trade_dt[trade_dt == '20190603'].index.values[0]]:
        # temp = CORN_K.fetch_optimal_portfolio(i, W, Pho, k, price_daily_wide, correlation[correlation.trade_dt == i])
        temp = Symmetric_CORN_K.fetch_optimal_portfolio(i, W, Pho, k, price_daily_wide, correlation[correlation.trade_dt == i])
        # temp = Functional_CORN_K.fetch_optimal_portfolio(i, W, Pho, Lambda, k, price_daily_wide,
        #                                                  correlation[correlation.trade_dt == i])
        s = s * np.dot(temp, price_daily[price_daily.trade_dt == i].ratio_dq_price)
        portfolio = portfolio.append({'trade_dt': i, 'portfolio': temp, 'RET': s}, ignore_index=True, sort=True)
        print('datachar= ' + i + ' has finished, s is ' + str(s))
        end = time.time()
        print('time cost is ' + str((end - start).__round__(2)))
        start = time.time()
    return portfolio


if __name__ == '__main__':
    a = main()
