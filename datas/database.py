# -*- coding: utf-8 -*- 
"""
author: Kyle Cai
e-mail: wycai@pku.edu.cn
"""

import pandas as pd
import pymysql
from WindPy import w
from sqlalchemy import create_engine

from configs.password import *
from datas.data_type import DataType


class DataBase:
    __base_params = {'table_name': str,
                     'db_name': str}

    def __new__(cls, *args, **kwargs):
        obj = super(DataBase, cls).__new__(cls)
        assert not [x for x in cls.get_base_params() if x not in kwargs.keys()], 'Key params missing.'
        return obj

    def __init__(self, **kwargs):
        self.data = None
        self.__dict__.update(**kwargs)

    def __repr__(self):
        raise NotImplementedError

    @classmethod
    def get_base_params(cls):
        return cls.__base_params

    def from_mysql(self, column: str):
        connection = pymysql.connect(host='localhost', user=db_user, password=db_password, db=self.db_name)
        sql = f"SELECT DISTINCT {column} FROM {self.table_name}"
        self.data = pd.read_sql(sql, con=connection)
        connection.close()
        return self.data

    def to_mysql(self):
        engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{host}:{port}/{self.db_name}?charset=utf8")
        self.data.to_sql(name=self.table_name, con=engine, if_exists='append', index=False)

    def to_mysql_from_csv(self, csv_file: str):
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = f.readline()
        columns = reader.split(',')
        table_column = ''
        for column in columns:
            table_column = table_column + column + ' varchar(255),'
        table_column = table_column[:-1]
        connection = pymysql.connect(host='localhost', user=db_user, password=db_password, db=self.db_name)
        cur = connection.cursor()
        create_sql = f"CREATE TABLE IF NOT EXISTS {self.table_name} ({table_column}) DEFAULT CHARSET=utf8"
        data_sql = f"LOAD DATA LOCAL INFILE '{self.data}' INTO TABLE {self.table_name} FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\r\\n' IGNORE 1 LINES"
        cur.execute(f'use {self.db_name}')
        cur.execute('SET NAMES utf8;')
        cur.execute('SET character_set_connection=utf8;')
        cur.execute(create_sql)
        cur.execute(data_sql)
        connection.commit()
        connection.close()
        cur.close()

    def store_to_csv(self, file_location: str) -> None:
        self.data.to_csv(file_location)

    def fetch_data(self, *args, **kwargs):
        raise NotImplementedError


class WSDData(DataBase):
    __base_params = {'data_type': DataType,
                     'code': str,
                     'field': str,
                     'table_name': str,
                     'db_name': str,
                     'start': str,
                     'end': str,
                     }

    def fetch_data(self, return_df=False, *args) -> pd.DataFrame:
        if not args:
            df = w.wsd(self.code, self.field, self.start, self.end, self.data_type.value, usedf=True)
        else:
            df = w.wsd(self.code, self.field, self.start, self.end, *args, usedf=True)
        if df[0] != 0:
            raise RuntimeError('Fetching data failed.')
        self.data = df[1]
        self.data['tradecode'] = self.code
        self.data['tradedate'] = self.data.index
        self.data = self.data.reset_index(drop=True)
        if return_df:
            return self.data

    def __repr__(self):
        return self.__class__.__name__ + ': ' + self.field


class WSETData(DataBase):
    __base_params = {'data_type': str,
                     'field': str,
                     'table_name': str,
                     'db_name': str,
                     }

    def fetch_data(self, return_df=False, *args) -> pd.DataFrame:
        df = w.wset(self.data_type.value, self.field, *args, usedf=True)
        if df[0] != 0:
            raise RuntimeError('Fetching data failed.')
        self.data = df[1]
        if return_df:
            return self.data

    def __repr__(self):
        return self.__class__.__name__ + ': ' + self.field

class WSSData(DataBase):
    __base_params = {'data_type': str,
                     'field': str,
                     'table_name': str,
                     'db_name': str,
                     }

    def fetch_data(self, return_df=False, *args) -> pd.DataFrame:
        df = w.wss(self.data_type.value, self.field, *args, usedf=True)
        if df[0] != 0:
            raise RuntimeError('Fetching data failed.')
        self.data = df[1]
        if return_df:
            return self.data

    def __repr__(self):
        return self.__class__.__name__ + ': ' + self.field

if __name__ == '__main__':
    Data()
