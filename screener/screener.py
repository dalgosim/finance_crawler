# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import datetime
import xgboost as xgb
import pickle

from dev_util.util import logger, mysql_manager, timer, config


class Screener:

    def __init__(self):
        self.logger = logger.APP_LOGGER
        self.model_name = config.CONFIG.MODEL_NAME
        self.model = self._load_model(self.model_name)
        self.mysql = mysql_manager.MysqlController()
        self.basis_date = config.BASIS_DATE
        if self.model is None:
            self.logger.error("The model is not exist. Please check your model name")

    def _load_model(self, model_name):
        model = None
        with open(f'finance_ml/models/{model_name}.pkl', 'rb') as f:
            model = pickle.load(f)
        return model

    def recommend(self, save=False):
        tables = config.CONFIG.MYSQL_CONFIG.TABLES
        query = f'''
            SELECT t3.cmp_nm_kor as cmp_nm_kor, m1.*
            FROM
                {tables.COMPANY_LIST_TABLE} t3, (SELECT t1.*, t2.close
                FROM {tables.METRIC_TABLE} t1, {tables.PRICE_TABLE} t2
                WHERE
                    t1.cmp_cd=t2.cmp_cd
                    AND t1.date=t2.date
                    AND t1.date='{self.basis_date}') m1
            WHERE
                t3.cmp_cd=m1.cmp_cd;'''

        # 데이터 가져와서
        metric_df = self.mysql.select_dataframe(query, log='screener')
        if len(metric_df) == 0:
            return None

        # infer
        metric_df = self._calc_metric(metric_df)
        metric_df = metric_df[['cmp_cd', 'date', 'close', 'PER', 'PCR', 'PBR', 'PSR', 'EV/EBITDA', 'ROE']]
        metric_df['pred'], metric_df['pos'], metric_df['neg'] = self.predict_stock(metric_df)
        metric_df['model'] = self.model_name

        if save:
            self.save(metric_df[['cmp_cd', 'date', 'pos', 'neg', 'pred', 'close', 'model']])

        return metric_df

    def predict_stock(self, metric_df):
        x = metric_df[['PER', 'PBR', 'PSR', 'PCR', 'ROE', 'EV/EBITDA']]
        X_real = xgb.DMatrix(x)

        pred_y = self.model.predict(X_real)
        pred = np.asarray([np.argmax(line) for line in pred_y])
        neg = np.asarray([line[0] for line in pred_y])
        pos = np.asarray([line[1] for line in pred_y])
        return pred, pos, neg

    def _calc_metric(self, df):
        df['close'] = df['close'].astype('int')
        df['PER'] = df['close']/df['EPS']
        df['PCR'] = df['close']/df['CFPS']
        df['PBR'] = df['close']/df['BPS']
        df['PSR'] = df['close']/df['SPS']
        return df

    def save(self, df):
        if df is not None and len(df) > 0:
            self.logger.debug(f'Recomm save start')
            table = config.CONFIG.MYSQL_CONFIG.TABLES.MODEL_RECOMMEND_TABLE
            self.mysql.insert_dataframe(df, table)
            self.logger.debug(f'Recomm save start')
        else:
            self.logger.debug(f'Recomm save fail : DataFrame is empty!')
