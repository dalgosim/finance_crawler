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
        self.prev_cmp_cd = self.__get_prev_comp_list()

    def __get_prev_comp_list(self):
        query = f'''
        SELECT cmp_cd FROM {self.table}
        WHERE date=(SELECT MAX(date) FROM {self.table});
        '''
        cmp_df = self.mysql.select_dataframe(query, log='get_prev_comp_list')
        cmp_cd_list = cmp_df['cmp_cd'].values.tolist()
        self.logger.debug(f'cmp_cd_list : {len(cmp_cd_list)}, {cmp_cd_list[:5]}')
        return cmp_cd_list

    def __crawl_stock_price(self, stock_code, max_page=250):
        sise_list = []
        page = 1
        last_date = ''
        while page <= max_page:
            _url = self.SISE_URL.format(code=stock_code, page=page)
            res = requests.get(_url)
            _list = self.__parse_sise_list(res.text)
            sise_list.extend(_list)
            if _list[0][0].startswith('2010.11') or _list[0][0] == last_date:
                break
            last_date = _list[0][0]
            page += 1
            timer.random_sleep(min_delay=self.delay)
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
        cmp_cd_list = common_sql.get_company_list(limit)['cmp_cd'].values
        for _, code in enumerate(cmp_cd_list):
            code = code[:6]
            stock_price = self.__crawl_stock_price(code, max_page=1)
            price_df = pd.DataFrame(stock_price, columns=self.headers)
            price_df = price_df.loc[price_df['date'] != '']
            price_df['date'] = price_df['date'].apply(lambda x: x.replace('.', '-'))
            for col in self.headers[1:]:
                price_df[col] = price_df[col].apply(lambda x: x.replace(',', ''))

            price_df = price_df.loc[price_df['date']>=self.basis_date]
            price_df['cmp_cd'] = code

            accum_df = pd.concat([accum_df, price_df], sort=False)
        self.logger.debug(f'Naver price crawling complete')

        if save:
            self.save(accum_df)
        return accum_df

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
