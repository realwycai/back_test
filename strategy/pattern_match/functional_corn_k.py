import pandas as pd
from scipy.optimize import minimize
import numpy as np


def fetch_optimal_portfolio(datechar, W, Pho, Lambda, k, price_daily_wide, correlation):
    expert = pd.DataFrame(columns=['w', 'pho', 'expert', 'success'])
    price_daily_wide = price_daily_wide.loc[price_daily_wide.index < datechar, :]
    price_daily_wide = price_daily_wide.loc[price_daily_wide.isna().sum(axis=1) == 0, :]
    t_1 = price_daily_wide.index[-1]
    price_daily_wide = price_daily_wide.T
    # 计算得到similar periods
    for w in W:
        coefficient = correlation.loc[correlation.w == w, 'coefficient'].iloc[0]
        coefficient = coefficient.loc[coefficient.index < t_1, coefficient.columns == t_1]
        coefficient = coefficient.reset_index(drop=False).rename(columns={t_1: 'coefficient'})
        for p in Pho:
            for lbda in Lambda:
                coefficient['v'] = 0
                coefficient.loc[coefficient.coefficient >= 0, 'v'] = [1 / (1 + np.exp(-lbda * (c - p))) for c in coefficient[coefficient.coefficient >= 0].coefficient]
                coefficient.loc[coefficient.coefficient < 0, 'v'] = [1 / (1 + np.exp(-lbda * (c + p))) - 1 for c in
                                                                     coefficient[coefficient.coefficient < 0].coefficient]
                coefficient=coefficient[coefficient.trade_dt.isin(price_daily_wide.columns)]
                C_x = np.mat(price_daily_wide.loc[:,price_daily_wide.columns.isin(coefficient.trade_dt)])
                # 得到expert
                b0 = np.zeros([len(C_x), 1])
                b0[0] = 1
                cons = ({'type': 'ineq', 'fun': lambda x: x}, {'type': 'eq', 'fun': lambda x: sum(x) - 1})
                res = minimize(lambda b: -np.dot(np.log(np.dot(b.T, C_x)), coefficient.v), b0, constraints=cons)
                expert = expert.append({'w': w, 'pho': p, 'lambda': lbda, 'expert': res.x.round(5), 'success': res.success},
                                       ignore_index=True)
    # 计算ensemble E_t
    expert['accumulated_return'] = 0
    expert['accumulated_return'] = np.dot(np.mat(list(expert.expert)), price_daily_wide).prod(axis=1)
    ensemble = expert.sort_values(by='accumulated_return', ascending=False).reset_index(drop=True).iloc[0:k]
    optimal_b = (ensemble.accumulated_return * ensemble.expert).sum() / ensemble.accumulated_return.sum()
    return optimal_b
