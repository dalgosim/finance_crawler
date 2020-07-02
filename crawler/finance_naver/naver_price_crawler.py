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

    def crawl(self, cmp_cd):
        code = cmp_cd[:6]
        self.logger.debug(f'Price crawling start ({self.basis_date})')

        stock_price = self.__crawl_stock_price(code, max_page=1)
        price_df = pd.DataFrame(stock_price, columns=self.headers)
        price_df = price_df.loc[price_df['date'] != '']
        price_df['date'] = price_df['date'].apply(lambda x: x.replace('.', '-'))
        for col in self.headers[1:]:
            price_df[col] = price_df[col].apply(lambda x: x.replace(',', ''))

        price_df = price_df.loc[price_df['date']==self.basis_date]
        self.logger.debug(f'Naver price crawling complete')
        return price_df
