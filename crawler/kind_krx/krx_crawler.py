# -*- coding: utf-8 -*-
import pandas as pd

from crawler import Crawler
from util import timer, logger, config, mysql_manager


class KRXCrawler(Crawler):
    '''comp.fnguid.com에서 metric_studio 관련 정보 가져오기'''
    STOCK_TYPE = {
        'kospi': 'stockMkt',
        'kosdaq': 'kosdaqMkt'
    }

    def __init__(self):
        self.logger = logger.APP_LOGGER
        self.mysql = mysql_manager.MysqlController()

    def _get_download_corplist(self, market_type='kospi'):
        self.logger.debug(f'{market_type} crawling start')
        fmt = '{:06d}.KS' if market_type == 'kospi' else '{:06d}.KQ'
        _market_type = self.STOCK_TYPE[market_type]
        
        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download'
        url = url + f'&marketType={_market_type}'
        _df = pd.read_html(url, header=0)[0]
        
        _df.종목코드 = _df.종목코드.map(fmt.format)
        self.logger.debug(f'{market_type} crawling finish')
        return _df

    def crawl(self, save=False):
        kospi_df = self._get_download_corplist('kospi')
        kosdaq_df = self._get_download_corplist('kosdaq')
        comp_df = pd.concat([kospi_df, kosdaq_df])
        comp_df = comp_df.rename(columns={'회사명':'cmp_nm_kor', '종목코드':'cmp_cd'})
        if save:
            self.save(comp_df)
        return comp_df

    def save(self, df):
        if df is not None:
            table = config.CONFIG.MYSQL_CONFIG.TABLES.COMPANY_LIST_TABLE
            self.mysql.insert_dataframe(df, table)
