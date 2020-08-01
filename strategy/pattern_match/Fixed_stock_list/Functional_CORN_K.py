import pandas as pd
from scipy.optimize import minimize
import numpy as np
import time


def fetch_optimal_portfolio(datechar, W, Pho, Lambda, k, price_daily_wide, correlation):
    expert = pd.DataFrame(columns=['w', 'pho', 'lambda', 'expert', 'success'])
    price_daily_wide = price_daily_wide[(price_daily_wide.index < datechar) & (price_daily_wide.index >= '20120104')].T
    # 计算得到similar periods
    for w in W:
        correlation_coefficient = correlation.loc[
            correlation['w'] == w, ~((correlation.columns == ('trade_dt')) | (correlation.columns == ('w')))].reset_index(
            drop=True)
        correlation_coefficient = (correlation_coefficient.T).reset_index(drop=False).rename(
            columns={0: 'coefficient', 'index': 'trade_dt'})
        correlation_coefficient = correlation_coefficient[correlation_coefficient.trade_dt < datechar]
        for p in Pho:
            for lbda in Lambda:
                correlation_coefficient['v'] = 0
                correlation_coefficient.loc[correlation_coefficient.coefficient >= 0, 'v'] = [1 / (1 + np.exp(-lbda * (c - p)))
                                                                                              for c
                                                                                              in correlation_coefficient[
                                                                                                  correlation_coefficient.coefficient >= 0].coefficient]
                correlation_coefficient.loc[correlation_coefficient.coefficient < 0, 'v'] = [1 / (1 + np.exp(-lbda * (c + p))) - 1
                                                                                             for c in correlation_coefficient[
                                                                                                 correlation_coefficient.coefficient < 0].coefficient]
                C_x = np.mat(price_daily_wide)
                # 得到expert
                b0 = np.zeros([len(C_x), 1])
                b0[:] = 1 / len(C_x)
                cons = ({'type': 'ineq', 'fun': lambda x: x}, {'type': 'eq', 'fun': lambda x: sum(x) - 1})
                res = minimize(lambda b: -np.dot(np.dot(b0.T, C_x), correlation_coefficient.v), b0, constraints=cons)
                expert = expert.append({'w': w, 'pho': p, 'lambda': lbda, 'expert': res.x.round(5), 'success': res.success},
                                       ignore_index=True)
    # 计算ensemble E_t
    expert['accumulated_return'] = 0
    expert['accumulated_return'] = np.dot(np.mat(list(expert.expert)), price_daily_wide).prod(axis=1)
    ensemble = expert.sort_values(by='accumulated_return', ascending=False).reset_index(drop=True).iloc[0:k]
    optimal_b = (ensemble.accumulated_return * ensemble.expert).sum() / ensemble.accumulated_return.sum()
    return optimal_b
