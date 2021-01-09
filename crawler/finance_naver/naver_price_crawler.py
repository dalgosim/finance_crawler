# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

from crawler import Crawler
from dev_util.util import timer, logger, config, mysql_manager, common_sql


class NaverPriceCrawler(Crawler):
    '''네이버 주식에서 직접 주가 정보 가져오기'''

    def __init__(self):
        super().__init__()
        self.SISE_URL = 'https://finance.naver.com/item/sise_day.nhn?code={code}&page={page}'
        self.headers = ['date', 'close', 'diff', 'open', 'high', 'low', 'volume']
        self.price_column = ['date', 'cmp_cd', 'open', 'close', 'high', 'low', 'volume']
        self.table = config.CONFIG.MYSQL_CONFIG.TABLES.PRICE_TABLE
        self.del_table = config.CONFIG.MYSQL_CONFIG.TABLES.COMPANY_DEL_LIST_TABLE
        self.del_cmp_cd = self.__get_del_comp_list()
        
        self.req_header = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36'}

    def __get_del_comp_list(self):
        query = f'''SELECT cmp_cd FROM {self.del_table};'''
        cmp_df = self.mysql.select_dataframe(query, log='get_del_comp_list')
        cmp_cd_list = cmp_df['cmp_cd'].values.tolist()
        self.logger.debug(f'del cmp_cd list : {len(cmp_cd_list)}, {cmp_cd_list[:5]}')
        return cmp_cd_list

    def __crawl_stock_price(self, full_code, max_page=250):
        stock_code = full_code[:6]
        sise_list = []
        page = 1
        last_date = ''
        while page <= max_page:
            _url = self.SISE_URL.format(code=stock_code, page=page)
            res = requests.get(_url, headers=self.req_header)
            _list = self.__parse_sise_list(res.text)

            # 페이지 정보가 없는 종목(ex. 상장폐지)
            if page==1 and len(_list)>0:
                if _list[0].count('') == 6:
                    self.save_del_stock(full_code)

            timer.random_sleep(min_delay=self.delay)

            sise_list.extend(_list)
            if len(_list)==0:
                break
            if _list[0][0].startswith('2010.11') or _list[0][0] == last_date:
                break
            last_date = _list[0][0]
            page += 1

        return sise_list

    def __parse_sise_list(self, res_text):
        item_list = []
        soup = BeautifulSoup(res_text, 'lxml')
        box_list = soup.find_all('table', attrs={'class': 'type2'})
        for box in box_list:
            tr = box.find_all('tr')
            for row in tr:
                td = row.find_all('td')
                if len(td) == 7:
                    items = [item.text.strip() for item in td]
                    item_list.append(items)
        return item_list

    def crawl(self, save=False):
        self.logger.debug(f'Price crawling start ({self.basis_date})')
        accum_df = pd.DataFrame([])
        limit = 5 if config.TEST_MODE else 0
        cmp_cd_list = list(common_sql.get_company_list(limit)['cmp_cd'].values)
        
        for del_code in self.del_cmp_cd:
            cmp_cd_list.remove(del_code)

        for i, full_code in enumerate(cmp_cd_list):
            if i%100==0:
                self.logger.debug(f'price crawling... ({i}/{len(cmp_cd_list)})')
            stock_price = self.__crawl_stock_price(full_code, max_page=1)
            price_df = pd.DataFrame(stock_price, columns=self.headers)
            price_df = price_df.loc[price_df['date'] != '']
            price_df['date'] = price_df['date'].apply(lambda x: x.replace('.', '-'))
            for col in self.headers[1:]:
                price_df[col] = price_df[col].apply(lambda x: x.replace(',', ''))

            price_df = price_df.loc[price_df['date']>=self.basis_date]
            price_df['cmp_cd'] = full_code

            accum_df = pd.concat([accum_df, price_df], sort=False)
        self.logger.debug(f'Naver price crawling complete')

        if save:
            self.save(accum_df)
        return accum_df
    
    def save_del_stock(self, code):
        self.logger.debug(f'Removed stock save start')
        df = pd.DataFrame([code], columns=['cmp_cd'])
        self.mysql.insert_dataframe(df, self.del_table)
        self.logger.debug(f'Removed stock save complete')

    def save(self, df):
        if df is not None:
            self.logger.debug(f'Price save start')
            # df['date'] = self.basis_date
            df.columns = [name.lower() for name in df.columns]
            df = df.rename(columns={"adj close": "adj_close"})
            df = df[self.price_column]
            df.to_csv(f'log/price_{self.basis_date}.csv', mode='w')
            self.mysql.insert_dataframe(df, self.table)
            self.logger.debug(f'Price save complete')
        else:
            self.logger.debug(f'Price save fail : DataFrame is empty!')
