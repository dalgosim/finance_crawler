# -*- coding: utf-8 -*-
import pandas as pd
import pandas_datareader as pdr
from datetime import datetime
from pandas_datareader._utils import RemoteDataError

from crawler import Crawler
from util import timer, logger, config, mysql_manager, common_sql


class DataReaderCrawler(Crawler):
    '''data_reader를 활용해서 주가 정보 가져오기'''

    def __init__(self):
        super().__init__()
        self.basis_date = config.BASIS_DATE
        self.table = config.CONFIG.MYSQL_CONFIG.TABLES.PRICE_TABLE

    def crawl(self, save=False):
        self.logger.debug(f'Price crawling start')
        start_date = end_date = self.basis_date

        price_df = pd.DataFrame([])
        limit = 0 if config.TEST_MODE else 5
        cmp_cd_list = common_sql.get_company_list(limit)['cmp_cd'].values
        for code in cmp_cd_list:
            try:
                _df = pdr.DataReader(code, 'yahoo', start_date, end_date)
            except RemoteDataError:
                # 상폐등으로 해당 기간에 데이터가 없을 때
                continue
            except KeyError:
                continue
            _df['cmp_cd'] = code
            price_df = pd.concat([price_df, _df])
        self.logger.debug(f'Price crawling complete')

        if save:
            self.save(price_df)
        return price_df

    def save(self, df):
        if df is not None:
            self.logger.debug(f'Price save start')
            df['date'] = self.basis_date
            df.columns = [name.lower() for name in df.columns]
            df = df.rename(columns={"adj close": "adj_close"})
            self.mysql.insert_dataframe(df, self.table)
            self.logger.debug(f'Price save complete')
        else:
            self.logger.debug(f'Price save fail : DataFrame is empty!')
