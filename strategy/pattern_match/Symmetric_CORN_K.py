import pandas as pd
from scipy.optimize import minimize
import numpy as np


def fetch_optimal_portfolio(datechar, W, Pho, k, price_daily_wide, correlation):
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
            similar_period_positive = coefficient[coefficient.coefficient > p].trade_dt
            similar_period_negative = coefficient[coefficient.coefficient < -p].trade_dt
            C_x_positive = price_daily_wide.loc[:, price_daily_wide.columns.isin(similar_period_positive)]
            C_x_negative = price_daily_wide.loc[:, price_daily_wide.columns.isin(similar_period_negative)]
            if len(C_x_negative) + len(C_x_negative) < 2 * k:
                continue
            # 得到expert
            b0 = np.zeros([len(C_x_positive), 1])
            b0[:] = 1 / len(C_x_positive)
            cons = ({'type': 'ineq', 'fun': lambda x: x}, {'type': 'eq', 'fun': lambda x: sum(x) - 1})
            res = minimize(
                lambda b: -np.log(np.dot(b.T, np.mat(C_x_positive))).sum() + np.log(np.dot(b.T, np.mat(C_x_negative))).sum(), b0,
                constraints=cons)
            expert = expert.append({'w': w, 'pho': p, 'expert': res.x.round(5), 'success': res.success}, ignore_index=True)
    # 计算ensemble E_t
    expert['accumulated_return'] = 0
    expert['accumulated_return'] = np.dot(np.mat(list(expert.expert)), price_daily_wide).prod(axis=1)
    ensemble = expert.sort_values(by='accumulated_return', ascending=False).reset_index(drop=True).iloc[0:k]
    optimal_b = (ensemble.accumulated_return * ensemble.expert).sum() / ensemble.accumulated_return.sum()
    return optimal_b
