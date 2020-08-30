# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd

from dev_util.util import logger, mysql_manager, timer, config
from dev_util.util import common_sql


class ChartIndexCalculator:

    def __init__(self):
        self.logger = logger.APP_LOGGER
        self.mysql = mysql_manager.MysqlController()
        self.basis_date = config.BASIS_DATE

    def calc_chart_index(self):
        comp_list = common_sql.get_company_list()['cmp_cd'].values
        comp_list = ['950200']
        for cmp_cd in comp_list:
            price_df = common_sql.get_recent_price(cmp_cd, days=121)
            price_df = price_df.sort_values('date').reset_index(drop=True)
            print(price_df.head())
            print(price_df.tail())
            price_df['RSI'] = self.calc_RSI(price_df['close'])
            print(price_df.tail(20))

    def calc_RSI(self, price_df, N=14):
        U = np.where(price_df.diff(1) > 0, price_df.diff(1), 0)
        D = np.where(price_df.diff(1) < 0, price_df.diff(1) * (-1), 0)

        AU = pd.DataFrame(U).rolling(window=N, min_periods=N).mean()
        AD = pd.DataFrame(D).rolling(window=N, min_periods=N).mean()
        RSI = AU.div(AD+AU) * 100
        return RSI
