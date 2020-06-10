# -*- coding: utf-8 -*-
import os
import requests
import pandas as pd
import json
import datetime
import re
from bs4 import BeautifulSoup

from util import timer, logger, config, mysql_manager, common_sql
from crawler import Crawler


class MetricCrawler(Crawler):
    '''comp.fnguid.com에서 metric_studio 관련 정보 가져오기'''
    FINANCE_RATIO_URL = 'http://comp.fnguide.com/SVO2/ASP/SVD_FinanceRatio.asp'
    INVEST_URL = 'http://comp.fnguide.com/SVO2/ASP/SVD_Invest.asp'

    # frq, frqTyp => 0:연간, 1:분기
    param = {
        'pGB': 'S7',
        'cID': 'S7',
        'MenuYn': 'N',
        'ReportGB': 'D',
        'NewMenuID': '15',
        'stkGb': '701',
    }

    def __init__(self):
        super().__init__()
        self.basis_date = config.BASIS_DATE
        self.table = config.CONFIG.MYSQL_CONFIG.TABLES.METRIC_TABLE

    def crawl(self, save=False):
        self.logger.debug(f'Fnguide crawling start')
        limit = 0 if config.TEST_MODE else 5
        result = []
        cmp_cd_list = common_sql.get_company_list(limit)['cmp_cd'].values
        for i, cmp_cd in enumerate(cmp_cd_list):
            if i%100==0:
                self.logger.debug(f'metric crawling... ({i}/{len(cmp_cd_list)})')
            fncode = cmp_cd[:6] # .KS, .KQ 태그 제외
            header = {
                'Host': 'comp.fnguide.com',
            }
            cmp_row = dict()
            gicode = 'A%06d'%int(fncode)
            cmp_row['cmp_cd'] = cmp_cd

            for url in [self.INVEST_URL, self.FINANCE_RATIO_URL]:
                res = requests.get(f'{url}?gicode={gicode}', headers=header)
                soup = BeautifulSoup(res.text, 'lxml')
                table_list = soup.find_all('table', attrs={'class': 'us_table_ty1'})
                for tb in table_list:
                    trs = tb.find_all('tr')[1:]
                    for tr in trs:
                        td = list(tr.children)

                        if int(td[1].attrs.get('colspan', 0)) > 0:
                            continue

                        # 지표 key-value
                        key = td[1].find_all('span', attrs={'class': 'txt_acd'})
                        if len(key) > 0:
                            key = key[0].text
                        else:
                            key = td[1].text
                        key = key.strip()

                        val = td[-2].text.strip()
                        if key == 'EV/EBITDA':  # 1년주기여서, 작년기준으로 사용
                            val = td[-4].text.strip()
                        if len(val) > 0:
                            try:
                                cmp_row[key] = float(val.replace(',', ''))
                            except:
                                pass
            result.append(cmp_row)
            timer.random_sleep(min_delay=self.delay)

        df_result = pd.DataFrame(result)
        try:
            df_result = df_result[['cmp_cd', 'EPS', 'CFPS', 'BPS', 'SPS', 'EV/EBITDA', 'ROE']]
            df_result = df_result.loc[df_result.isnull().sum(axis=1)<6, :]
        except KeyError:
            self.logger.debug(f"{df_result[['cmp_cd']]}, KeyError : ['EV/EBITDA'] not in index")
            df_result = None
        self.logger.debug(f'Fnguide crawling complete')

        if save:
            self.save(df_result)

        return df_result
    
    def save(self, df):
        if df is not None:
            self.logger.debug(f'Fnguide save start')
            df['date'] = self.basis_date
            self.mysql.insert_dataframe(df, self.table)
            self.logger.debug(f'Fnguide save complete')
        else:
            self.logger.debug(f'Fnguide save fail : DataFrame is empty!')
