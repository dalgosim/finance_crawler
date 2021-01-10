# -*- coding: utf-8 -*-
import os
import requests
import pandas as pd
import json
import datetime
import time

from crawler import Crawler
from dev_util.util import timer, logger, config, mysql_manager, common_sql


class RecommendationItemCrawler(Crawler):
    in_cols = [
        'anl_dt', 'brk_cd', 'brk_nm_kor', 'cmp_cd', 'cmp_nm_kor',
        'in_diff_reason', 'in_dt', 'num', 'pf_cd', 'pf_nm_kor',
        'pre_dt', 'reason_in', 'totrow', #'in_adj_price',
        # 'cnt', 'file_nm', 'pre_adj_price',
        # 'recommend_adj_price', 'recomm_price', 'recomm_rate',
        # 'in_dt_crawl',
        'recomm_price'
    ]
    out_cols = [
        'brk_cd', 'cmp_cd', 'pf_cd', 'in_dt', 'out_dt', 'diff_dt',
        'reason_out', 'out_adj_price', 'accu_rtn', 
        # 'out_dt_crawl'
    ]
    param = {
        'startDt': '20190107',
        'endDt': '20190207',
        'brkCd': 0,
        'pfCd': 0,
        'perPage': 20,
        'curPage': 1,
        'proc': 1, #신규추천:1, 추천제외:2
    }

    def __init__(self, delay=1):
        super().__init__()
        self.RECOMM_URL = 'https://recommend.wisereport.co.kr/v1/Home/GetInOut'
        self.in_table = config.CONFIG.MYSQL_CONFIG.TABLES.NAVER_IN_TABLE
        self.out_table = config.CONFIG.MYSQL_CONFIG.TABLES.NAVER_OUT_TABLE
        
        self.full_cmp_cd_list = dict()
        for cmp_cd in common_sql.get_company_list()['cmp_cd'].values:
            self.full_cmp_cd_list[str(cmp_cd[:6])] = cmp_cd

    def __find_full_cmp_cd(self, cmp_cd):
        return self.full_cmp_cd_list.get(cmp_cd, cmp_cd)

    def __crawl(self, date, proc):
        self.param['startDt'] = date
        self.param['endDt'] = date
        self.param['proc'] = proc
        curr_page = 1
        max_page = 2
        item_df = pd.DataFrame([])
        while curr_page <= max_page:
            self.param['curPage'] = curr_page
            res = requests.post(self.RECOMM_URL, data=self.param)
            res = json.loads(res.text)
            if len(res['data']) > 0:
                res = res['data']
                res = pd.DataFrame.from_dict(res)
                item_df = pd.concat([item_df, res])
                max_page = int(res['TOTROW'].values[0])
            curr_page += 1

        timer.random_sleep(min_delay=self.delay)
        item_df.columns = map(str.lower, item_df.columns)
        return item_df

    def crawl(self, proc=1, save=False):
        self.logger.debug(f'RecommendationItem crawling start (proc : {proc}, {self.basis_date})')
        _date = self.basis_date.replace('-', '')

        # crawl
        recom_df = self.__crawl(_date, proc)

        if len(recom_df) > 0:
            recom_df = recom_df[self.in_cols] if proc==1 else recom_df[self.out_cols]
            recom_df['cmp_cd'] = recom_df['cmp_cd'].apply(lambda x: self.__find_full_cmp_cd(x))
        self.logger.debug(f'RecommendationItem crawling complete (proc : {proc}, {self.basis_date}) : {len(recom_df)}')

        if save:
            self.save(recom_df, proc)
        return recom_df

    def save(self, df, proc):
        if df is not None:
            self.logger.debug(f'Recom save start')
            # df['date'] = self.basis_date
            df.columns = [name.lower() for name in df.columns]
            if proc==1:
                self.mysql.insert_dataframe(df, self.in_table)
                self.logger.debug(f'Recom_in save complete')
            else:
                self.mysql.insert_dataframe(df, self.out_table)
                self.logger.debug(f'Recom_out save complete')
        else:
            self.logger.debug(f'Recom save fail : DataFrame is empty!')
