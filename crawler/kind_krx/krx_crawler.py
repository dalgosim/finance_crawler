# -*- coding: utf-8 -*-
import pandas as pd
import FinanceDataReader as fdr

from crawler import Crawler
from dev_util.util import timer, logger, config


class KRXCrawler(Crawler):
    '''KRX에서 상장종목 정보 가져오기'''

    COL_DICT = {
        'Symbol':'cmp_cd',
        'Market':'market',
        'Name':'cmp_nm_kor',
        'Sector':'sector',
        'Industry':'industry', 
        'ListingDate':'regi_date', # 상장일
        'SettleMonth':'settlement_month', # 결산월
        'Representative':'representative',
        'HomePage':'website',
        'Region':'location',
    }
    CODE_DICT = {
        'KOSPI':'KS',
        'KOSDAQ':'KQ',
        'KONEX':'KX',
    }

    def __init__(self):
        super().__init__()
        self.table = config.CONFIG.MYSQL_CONFIG.TABLES.COMPANY_LIST_TABLE

    def _get_download_corplist(self, market_type='KRX'):
        self.logger.debug(f'{market_type} crawling start')
        
        df = fdr.StockListing(market_type) # KRX, KOSPI, KOSDAQ
        df = df.rename(columns=self.COL_DICT)
        
        # 파생상품은 코드가 73501BA2 형태로 되어있어서 제외
        df = df[df.cmp_cd.str.len()==6]

        df['cmp_cd'] = df.apply(lambda row: f"{row[0]}.{self.CODE_DICT.get(row[1], '')}", axis=1)

        self.logger.debug(f'{market_type} crawling finish')
        return df

    def crawl(self, save=False):
        comp_df = self._get_download_corplist()

        if save:
            self.save(comp_df)
        return comp_df

    def save(self, df):
        if df is not None:
            self.logger.debug(f'KRX save start')
#             df.to_csv(f'log/comp_{self.basis_date}.csv', mode='w')
            self.mysql.insert_dataframe(df, self.table)
            self.logger.debug(f'KRX save complete')
        else:
            self.logger.debug(f'KRX save fail : DataFrame is empty!')
