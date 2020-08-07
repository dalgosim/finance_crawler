# -*- coding: utf-8 -*-
import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

from crawler import Crawler
from dev_util.util import timer, logger, config, mysql_manager, common_sql


class NaverReportCrawler(Crawler):
    '''네이버 주식에서 직접 주가 정보 가져오기'''

    def __init__(self):
        super().__init__()
        self.REPORT_URL = 'https://finance.naver.com/research/company_list.nhn?searchType=writeDate&writeFromDate={date}&writeToDate={date}&page={page}'
        self.DETAIL_URL = 'https://finance.naver.com/research/company_read.nhn?nid={rpt_cd}'
        self.table = config.CONFIG.MYSQL_CONFIG.TABLES.REPORT_TABLE
        self.headers = ['cmp_nm_kor', 'title', 'brk_nm_kor', 'pdf', 'write_date',
                        'count', 'cmp_cd', 'report_id', 'goal_price', 'opinion',
                        'body']
        self.check_redun = dict()
        self.cmp_patt = re.compile("code=([0-9]{6})")
        self.nid_patt = re.compile("nid=([0-9]{5})")

    def __crawl_report(self, max_page=10):
        report_list = []
        page = 1
        while page <= max_page:
            _url = self.REPORT_URL.format(date=self.basis_date, page=page)
            res = requests.get(_url)
            _list = self.__parse_report_list(res.text)
            report_list.extend(_list)
            if len(_list) == 0:
                break
            page += 1
            timer.random_sleep(min_delay=self.delay)
        return report_list

    def __parse_report_list(self, res_text):
        item_list = []
        soup = BeautifulSoup(res_text, 'lxml')
        box_list = soup.find_all('table', attrs={'class': 'type_1'})
        for box in box_list:
            tr = box.find_all('tr')
            for row in tr:
                td = row.find_all('td')
                if len(td) == 6:
                    items = [item.text.strip() for item in td]
                    cmp_cd = self.cmp_patt.search(td[0].a.attrs['href']).group(1)       # td[0] == 종목명
                    report_code = self.nid_patt.search(td[1].a.attrs['href']).group(1)  # td[1] == 제목
                    items.append(cmp_cd)
                    items.append(report_code)
                    if self.check_redun.get(report_code, True): # 중복 리포트는 제외
                        items.extend(self.__crawl_detail(report_code))
                        item_list.append(items)
                        self.check_redun[report_code] = False
                    break
        return item_list

    def __parse_detail_page(self, res_text):
        soup = BeautifulSoup(res_text, 'lxml')
        goal_price = soup.find_all('em', attrs={'class': 'money'})[0].text.strip().replace('원', '')
        opinion = soup.find_all('em', attrs={'class': 'coment'})[0].text.strip()
        body = soup.find_all('td', attrs={'class': 'view_cnt'})[0]
        body_cont = '\n'.join([p.text.strip() for p in body.find_all('p')])
        return goal_price, opinion, body_cont

    def __crawl_detail(self, rpt_cd):
        _url = self.DETAIL_URL.format(rpt_cd=rpt_cd)
        res = requests.get(_url)
        goal_price, opinion, body = self.__parse_detail_page(res.text)
        timer.random_sleep(min_delay=self.delay)
        return [goal_price, opinion, body]

    def crawl(self, save=False):
        self.logger.debug(f'Naver report crawling start ({self.basis_date})')

        analyst_report = self.__crawl_report(max_page=10)
        report_df = pd.DataFrame(analyst_report, columns=self.headers)
        # report_df = report_df.loc[report_df['date'] != '']
        # report_df['date'] = report_df['date'].apply(lambda x: x.replace('.', '-'))
        # for col in self.headers[1:]:
        #     report_df[col] = report_df[col].apply(lambda x: x.replace(',', ''))

        # report_df = report_df.loc[report_df['date']==self.basis_date]
        self.logger.debug(f'Naver report crawling complete')

        if save:
            self.save(report_df)
        return report_df

    def save(self, df):
        if df is not None:
            self.logger.debug(f'Report save start')
            df['date'] = self.basis_date
            df.columns = [name.lower() for name in df.columns]
            df = df.rename(columns={"title": "rpt_title",
                "body":"rpt_body", "pdf":"pdf_url",
                "write_date":"in_dt", "brk_nm_kor":"brk_cd",
                "report_id":"rpt_cd", "date":"in_dt"})
            df = df.drop(['cmp_nm_kor'], axis=1)
            # df.to_csv(f'log/price_{self.basis_date}.csv', mode='w')
            self.mysql.insert_dataframe(df, self.table)
            self.logger.debug(f'Report save complete')
        else:
            self.logger.debug(f'Report save fail : DataFrame is empty!')
