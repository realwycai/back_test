import os
import pandas as pd
import psycopg2
import numpy as np
import time
import CORN_K
import Symmetric_CORN_K
import Functional_CORN_K

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


# 以上证50 000016.SH 在2010年初20100104的流通市值前30支股为dataset
# 测试HSI
# 在上证50中动态选股

def main():
    W = [1, 4, 7, 10, 15]
    Pho = [0,  0.3, 0.5]
    Lambda = [10, 100, 500]
    k = 3
    # 选择股票范围
    MYDB = psycopg2.connect(database="MYDB", user="postgres", password="pgread", host="localhost", port="5432")
    sql = """SELECT * FROM china_index_component WHERE s_info_windcode='000016.SH'"""
    index_component = pd.read_sql(sql, MYDB)
    index_component.s_con_outdate = index_component.s_con_outdate.fillna('')
    index_component = index_component.loc[((index_component.s_con_outdate >= '20100104') | (index_component.s_con_outdate == '')),
                      :]
    sql = """SELECT s_info_windcode, trade_dt, s_dq_adjclose FROM china_stock_tradesdaily WHERE trade_dt>='20060101'"""
    price_daily = pd.read_sql(sql, MYDB)
    price_daily = price_daily[price_daily.s_info_windcode.isin(index_component.s_con_windcode)]
    # 计算behavior
    price_daily = price_daily.groupby(by='s_info_windcode').apply(lambda x: x.sort_values('trade_dt', ascending=True))
    price_daily = price_daily.set_index('trade_dt', drop=True)
    price_daily = price_daily.groupby(by='s_info_windcode').apply(
        lambda x: x.s_dq_adjclose / x.s_dq_adjclose.shift(1)).reset_index(
        drop=False)
    price_daily = price_daily.sort_values(by=['trade_dt', 's_info_windcode']).rename(columns={'s_dq_adjclose': 'ratio_dq_price'})
    trade_dt = price_daily.trade_dt.drop_duplicates().sort_values(ascending=True).reset_index(drop=True)
    price_daily_wide = price_daily.pivot(index='trade_dt', columns='s_info_windcode', values='ratio_dq_price')
    # 选择股票范围 恒生指数
    # price_daily_wide=pd.read_excel('C:/Users/wycai/Desktop/中信/corn-K/IndexData.xlsx',sheet_name='price',header=3)
    # price_daily_wide.Date=price_daily_wide.Date.apply(lambda x: str(x)[0:4]+str(x)[5:7]+str(x)[8:10])
    # price_daily_wide.iloc[:,1:]=price_daily_wide.iloc[:,1:]/price_daily_wide.iloc[:,1:].shift(1)
    # price_daily_wide=price_daily_wide.rename(columns={'Date':'trade_dt'})
    # price_daily=price_daily_wide.melt(id_vars='trade_dt',var_name='s_info_windcode',value_name='ratio_dq_price')
    # trade_dt = price_daily.trade_dt.drop_duplicates().sort_values(ascending=True).reset_index(drop=True)
    # price_daily_wide=price_daily_wide.set_index('trade_dt')
    # 计算相关系数
    s_con_indate = index_component.s_con_indate.unique()
    s_con_indate.sort()
    # s_con_indate = s_con_indate[(s_con_indate >= '20141215') & (s_con_indate <= '20160613')]
    s_con_indate = s_con_indate[s_con_indate >= '20090101']
    correlation = pd.DataFrame(columns=['coefficient', 's_con_indate', 'w'])
    for w in W:
        for i in s_con_indate:
            component = index_component.loc[(index_component.s_con_indate <= i) & (
                    (index_component.s_con_outdate >= i) | (index_component.s_con_outdate == '')), :].s_con_windcode
            price_daily_wide_temp = price_daily_wide.loc[:, price_daily_wide.columns.isin(component)]
            earning_matrix = price_daily_wide_temp.shift(1)
            for j in range(1, w):
                earning_matrix = pd.concat([earning_matrix, price_daily_wide_temp.shift(j + 1)], axis=1)
            # earning_matrix = earning_matrix.loc[earning_matrix.isna().sum(axis=1) == 0, :]
            coefficient = earning_matrix.T.corr()
            correlation = correlation.append({'coefficient': coefficient, 's_con_indate': i, 'w': w}, ignore_index=True)
            print(i + ' ' + str(w) + ' has finished.')
    temp = correlation.loc[:, ['s_con_indate', 'w']]
    temp = temp.groupby(by='w').apply(lambda x: x.s_con_indate.shift(1)).reset_index(drop=False)
    correlation['next_s_con_indate'] = temp.s_con_indate
    correlation.next_s_con_indate = correlation.next_s_con_indate.fillna('')
    # 计算收益
    s1 = 10000
    s2 = 10000
    s3 = 10000
    # portfolio = pd.DataFrame(columns=['trade_dt', 'portfolio1', 'RET1', 'portfolio2', 'RET2', 'portfolio3', 'RET3'])
    portfolio = pd.DataFrame(columns=['trade_dt', 'portfolio1', 'RET1'])
    start = time.time()
    for i in trade_dt[trade_dt[trade_dt == '20160104'].index.values[0]:trade_dt[trade_dt == '20181228'].index.values[0]]:
        component = index_component.loc[(index_component.s_con_indate <= i) & (
                (index_component.s_con_outdate >= i) | (index_component.s_con_outdate == '')), :]
        price_daily_wide_temp = price_daily_wide.loc[
            price_daily_wide.index <= i, price_daily_wide.columns.isin(component.s_con_windcode)]
        correlation_temp = correlation.loc[
            (correlation.s_con_indate <= i) & ((correlation.next_s_con_indate > i) | (correlation.next_s_con_indate == '')), [
                'coefficient', 'w']]
        # temp1 = CORN_K.fetch_optimal_portfolio(i, W, Pho, k, price_daily_wide_temp, correlation_temp)
        # s1 = s1 * np.dot(temp1, price_daily_wide_temp.loc[price_daily_wide.index == i, :].T)
        temp2 = Symmetric_CORN_K.fetch_optimal_portfolio(i, W, Pho, k, price_daily_wide_temp, correlation_temp)
        s2 = s2 * np.dot(temp2, price_daily_wide_temp.loc[price_daily_wide.index == i, :].T)
        # temp3 = Functional_CORN_K.fetch_optimal_portfolio(i, W, Pho, Lambda, k, price_daily_wide_temp, correlation_temp)
        # s3 = s3 * np.dot(temp3, price_daily_wide_temp.loc[price_daily_wide.index == i, :].T)
        # portfolio = portfolio.append(
        #     {'trade_dt': i, 'portfolio1': temp1, 'RET1': s1[0], 'portfolio2': temp2, 'RET2': s2[0], 'portfolio3': temp3,
        #      'RET3': s3[0]},
        #     ignore_index=True, sort=True)
        portfolio = portfolio.append({'trade_dt': i, 'portfolio1': temp2, 'RET1': s2[0]}, ignore_index=True, sort=True)
        # print('datachar= ' + i + ' has finished, s1 is ' + str(s1[0]) + ' s2 is ' + str(s2[0]) + ' s3 is ' + str(s3[0]))
        print('datachar= ' + i + ' has finished, s1 is ' + str(s2[0]))
        end = time.time()
        print('time cost is ' + str((end - start).__round__(2)))
        start = time.time()
    return portfolio


if __name__ == '__main__':
    a = main()
