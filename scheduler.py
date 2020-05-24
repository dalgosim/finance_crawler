# -*- coding: utf-8 -*-
import pickle
import pandas as pd
import datetime

from crawler.comp_fnguide import metric_daily
from util import const, mysql_manager, timer


def get_company_list():
    _mysql = mysql_manager.MysqlController()
    query = f'''SELECT distinct cmp_cd FROM {const.COMPANY_LIST_TABLE};'''
    return _mysql.select_dataframe(query)

def crawl_daily_metric():
    sp = metric_daily.MetricCrawler()
    cmp_list = get_company_list()['cmp_cd'].values.tolist()

    result_df = pd.DataFrame([])
    for _cmp in cmp_list:
        _df = sp.crawl(_cmp[1:])
        result_df = pd.concat([result_df, _df])
    result_df['date'] = timer.get_now('%Y-%m-%d')
    sp.save(result_df)
    print(f'crawl_daily_metric job done : {datetime.datetime.now()}')
