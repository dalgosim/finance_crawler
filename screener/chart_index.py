# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
from ta.trend import SMAIndicator
from ta.volatility import BollingerBands
from ta.trend import macd, macd_signal
from ta.momentum import RSIIndicator
from ta.trend import PSARIndicator
from ta.utils import dropna


from dev_util.util import logger, mysql_manager, timer, config
from dev_util.util import common_sql


class ChartIndexCalculator:

    def __init__(self):
        self.logger = logger.APP_LOGGER
        self.mysql = mysql_manager.MysqlController()
        self.basis_date = config.BASIS_DATE
        self.table = config.CONFIG.MYSQL_CONFIG.TABLES.CHART_INDEX_TABLE
        self.days = 121

    def calc_chart_index(self, save=False):
        ta_df = pd.DataFrame([])
        comp_list = common_sql.get_company_list_without_del()['cmp_cd'].values
        for cmp_cd in comp_list:
            price_df = common_sql.get_recent_price(cmp_cd, self.basis_date, days=self.days)
            price_df = price_df.sort_values('date').reset_index(drop=True)
            ta_feature = self.extract_ta_feature(price_df, self.days)
            ta_df = pd.concat([ta_df, ta_feature], sort=False)

        if save:
            self.save(ta_df)
        return ta_df

    def extract_ta_feature(self, price_df, days=120):
        if len(price_df) >= 0:
            # MA
            for w in [5, 10, 20, 60, 120]:
                indicator_ma = SMAIndicator(close=price_df["close"], window=w)
                price_df[f'ma{w}'] = indicator_ma.sma_indicator()

            # BB
            indicator_bb = BollingerBands(close=price_df["close"], window=20, window_dev=2)
            price_df['bb_mid'] = indicator_bb.bollinger_mavg()
            price_df['bb_high'] = indicator_bb.bollinger_hband()
            price_df['bb_low'] = indicator_bb.bollinger_lband()

            # MACD
            price_df['macd'] = macd(price_df['close'], window_slow=26, window_fast=12)
            price_df['macd_signal'] = macd_signal(price_df['close'], window_slow=26, window_fast=12, window_sign=9)

            # RSI
            indicator_rsi = RSIIndicator(price_df['close'], window=14)
            rsi_signal = SMAIndicator(indicator_rsi.rsi(), window=9)
            price_df['rsi'] = indicator_rsi.rsi()
            price_df['rsi_signal'] = rsi_signal.sma_indicator()

            # Psar
            indicator_psar = PSARIndicator(high=price_df['high'], low=price_df['low'], close=price_df['close'], step=0.02, max_step=0.2)
            price_df['psar_down'] = indicator_psar.psar_down()
            price_df['psar_up'] = indicator_psar.psar_up()

            price_df = price_df.drop(['open', 'close', 'high', 'low', 'volume', 'adj_close'], axis=1)
            price_df = price_df.reset_index(drop=True)
            price_df = price_df.tail(2) # 마지막 2개가 최종 결과, 1개만 뽑아도 되나 혹시 모르니 2개 생성해서 중복 insert함

        return price_df

    def save(self, df):
        if df is not None or len(df)>0:
            self.logger.debug(f'Chart index save start')
            df.columns = [name.lower() for name in df.columns]
            self.mysql.insert_dataframe(df, self.table)
            self.logger.debug(f'Chart index save complete : {len(df)}')
        else:
            self.logger.debug(f'chart index save fail : DataFrame is empty!')