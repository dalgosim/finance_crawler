# -*- coding: utf-8 -*-
import pickle
import pandas as pd
import datetime
from apscheduler.schedulers.background import BackgroundScheduler

import crawler
from crawler.comp_fnguide import metric_crawler
from crawler.kind_krx import krx_crawler
from crawler.data_reader import price_crawler
from util import mysql_manager, timer, logger, config

#################
# 1. 매일 KRX에서 종목명/코드 수집 : krx_crawler
# 2. 종목별 주가 수집
# 3. 종목별 fnguide 정보 수집 : metric_crawler
# 4. 네이버 종목 분석 게시판 수집(애널리스트 리포트)
# 5. 네이버에서 테마 정보 수집
# 6. 네이버 종목 게시판의 게시글 조회수 수집(관심도)
#################
_logger = logger.APP_LOGGER

def crawl_krxcode_daily():
    '''KRX에서 종목 코드 가져오기'''
    krx = krx_crawler.KRXCrawler()
    krx.crawl(save=True)
    _logger.debug(f'crawl_krxcode_daily job done')

def crawl_price_daily():
    '''yahoo finance에서 일자별 가격정보 가져오기'''
    drc = price_crawler.DataReaderCrawler()
    drc.crawl(save=True)
    _logger.debug(f'crawl_krxcode_daily job done')

def crawl_metric_daily():
    '''fnguide에서 재무제표 가져오기'''
    sp = metric_crawler.MetricCrawler()
    sp.crawl(save=True)
    _logger.debug(f'crawl_metric_daily job done')

def update_date():
    config.BASIS_DATE = timer.get_now('%Y-%m-%d')

# scheduler
def scheduler():
    period = config.CONFIG.PERIOD
    sched = BackgroundScheduler({'apscheduler.timezone': 'Asia/Seoul'})
    sched.add_job(update_date, 'cron', day_of_week=period.update_date.day_of_week, hour=period.update_date.hour)
    sched.add_job(crawl_krxcode_daily, 'cron', day_of_week=period.krx_crawler.day_of_week, hour=period.krx_crawler.hour)
    # sched.add_job(crawl_metric_daily, 'cron', day_of_week=weekday, hour='22')
    # sched.add_job(scheduler.infer_model, 'cron', day_of_week=weekday, hour='9')
    # sched.add_job(scheduler.crawl_daily_inout, 'cron', day_of_week=weekday, hour='9-15', minute='0-59/30')
    # sched.add_job(scheduler.crawl_daily_price, 'cron', day_of_week=weekday, hour='15', minute='40')
    sched.start()

def unit_test():

    # crawl_krxcode_daily() # 1
    crawl_price_daily() # 2
    # crawl_metric_daily() # 3
    pass

if __name__ == '__main__':
    config.load_config(run_type='test') # test, real
    print(f'===== TEST_MODE : {config.TEST_MODE} =====')
    # config.CONFIG.pprint(pformat='json')
    # scheduler()
    unit_test()
