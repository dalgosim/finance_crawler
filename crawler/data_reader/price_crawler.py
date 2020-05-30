# -*- coding: utf-8 -*-
import pandas as pd
import pandas_datareader as pdr
from datetime import datetime
from pandas_datareader._utils import RemoteDataError

from util import timer, logger, config #, mysql_controller
from crawler import Crawler


class DataReaderCrawler(Crawler):
    '''data_reader를 활용해서 주가 정보 가져오기'''

    def __init__(self):
        self.start_date = datetime(2005, 1, 1)
        self.end_date = datetime(2019, 12, 31)

    def crawl(self, comp_code_list, start_date=None, end_date=None):
        if start_date is None:
            start_date = timer.get_now('%Y-%m-%d')
        if start_date is None:
            end_date = start_date

        price_df = pd.DataFrame([])
        for _, code in enumerate(comp_code_list):
            try:
                _df = pdr.DataReader(code, 'yahoo', start_date, end_date)
            except RemoteDataError:
                # 상폐등으로 해당 기간에 데이터가 없을 때
                continue
            except KeyError:
                continue
            _df['code'] = code[:6]
            price_df = pd.concat([price_df, _df])

        return price_df

    def save(self):
        pass
