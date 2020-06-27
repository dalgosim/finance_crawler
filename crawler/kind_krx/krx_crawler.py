# -*- coding: utf-8 -*-
import pandas as pd

from crawler import Crawler
from dev_util.util import timer, logger, config


class KRXCrawler(Crawler):
    '''comp.fnguid.com에서 metric_studio 관련 정보 가져오기'''
    STOCK_TYPE = {
        'kospi': 'stockMkt',
        'kosdaq': 'kosdaqMkt'
    }
    COL_DICT = {
        '회사명':'cmp_nm_kor',
        '종목코드':'cmp_cd',
        '업종':'category',
        '주요제품':'major_product',
        '상장일':'regi_date',
        '결산월':'settlement_month',
        '대표자명':'representative',
        '홈페이지':'website',
        '지역':'location',
    }

    def __init__(self):
        super().__init__()

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
        comp_df = comp_df.rename(columns=self.COL_DICT)

        if save:
            self.save(comp_df)
        return comp_df

    def save(self, df):
        if df is not None:
            self.logger.debug(f'KRX save start')
            table = config.CONFIG.MYSQL_CONFIG.TABLES.COMPANY_LIST_TABLE
            self.mysql.insert_dataframe(df, table)
            self.logger.debug(f'KRX save start')
        else:
            self.logger.debug(f'KRX save fail : DataFrame is empty!')
