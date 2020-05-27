# -*- coding: utf-8 -*-
import pickle
import pandas as pd
import datetime
from apscheduler.schedulers.background import BackgroundScheduler

from crawler.comp_fnguide import metric_crawler
from util import const, mysql_manager, timer

#################
# 1. 매일 KRX에서 종목명/코드 수집
# 2. 종목별 주가 수집
# 3. 종목별 fnguide 정보 수집
# 4. 네이버 종목 분석 게시판 수집
# 5. 네이버에서 테마 정보 수집
# 6. 네이버 종목 게시판의 게시글 조회수 수집(관심도)
#################

def get_company_list():
    _mysql = mysql_manager.MysqlController()
    query = f'''SELECT distinct cmp_cd FROM {const.COMPANY_LIST_TABLE};'''
    return _mysql.select_dataframe(query)

def crawl_metric_daily():
    sp = metric_crawler.MetricCrawler()
    cmp_list = get_company_list()['cmp_cd'].values.tolist()

    result_df = pd.DataFrame([])
    for _cmp in cmp_list:
        _df = sp.crawl(_cmp[1:])
        result_df = pd.concat([result_df, _df])
    result_df['date'] = timer.get_now('%Y-%m-%d')
    sp.save(result_df)
    print(f'crawl_daily_metric job done : {datetime.datetime.now()}')


# scheduler
def scheduler():
    weekday = 'mon-fri'
    sched = BackgroundScheduler({'apscheduler.timezone': 'Asia/Seoul'})
    # sched.add_job(scheduler.run_screener, 'cron', day_of_week=weekday, hour='9')
    # sched.add_job(scheduler.infer_model, 'cron', day_of_week=weekday, hour='9')
    # sched.add_job(scheduler.crawl_daily_inout, 'cron', day_of_week=weekday, hour='9-15', minute='0-59/30')
    # sched.add_job(scheduler.crawl_daily_price, 'cron', day_of_week=weekday, hour='15', minute='40')
    sched.add_job(crawl_metric_daily, 'cron', day_of_week=weekday, hour='22')
    sched.start()

if __name__ == '__main__':
    scheduler()
