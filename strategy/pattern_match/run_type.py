# -*- coding: UTF-8 -*-
"""
author: Kyle Cai
e-mail: wycai@pku.edu.cn
"""
from enum import Enum, unique

@unique
class RunType(Enum):
    CORN_K = 'CORN_K'
    Symmetric_CORN_K = 'Symmetric_CORN_K'
    Functional_CORN_K = 'Functional_CORN_K'