# -*- coding: utf-8 -*- 
"""
author: Kyle Cai
e-mail: wycai@pku.edu.cn
"""

from enum import Enum


class DataType(Enum):
    daily_trading = "PriceAdj=F"
    financial_report = 'unit=1;rptType=1;Period=Q;PriceAdj=F;Days=Alldays'
    index_component = 'sectorconstituent'