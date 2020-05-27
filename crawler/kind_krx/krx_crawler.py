# -*- coding: utf-8 -*-
import pandas as pd
from util import timer, logger, const #, mysql_controller
from crawler import Crawler


class KRXCrawler(Crawler):
    '''comp.fnguid.com에서 metric_studio 관련 정보 가져오기'''
    STOCK_TYPE = {
        'kospi': 'stockMkt',
        'kosdaq': 'kosdaqMkt'
    }

    def __init__(self):
        pass

    def _get_download_corplist(self, market_type='kospi'):
        fmt = '{:06d}.KS' if market_type == 'kospi' else '{:06d}.KQ'
        market_type = self.STOCK_TYPE[market_type]
        
        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download'
        url = url + f'&marketType={market_type}'
        _df = pd.read_html(url, header=0)[0]
        
        _df.종목코드 = _df.종목코드.map(fmt.format)
        return _df

    def crawl(self):
        kospi_df = self._get_download_corplist('kospi')
        kosdaq_df = self._get_download_corplist('kosdaq')
        comp_df = pd.concat([kospi_df, kosdaq_df])
        comp_df = comp_df.rename(columns={'회사명':'name_kor', '종목코드':'cmp_cd'})
        return comp_df

    def save(self):
        pass
