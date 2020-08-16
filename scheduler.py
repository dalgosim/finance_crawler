# -*- coding: utf-8 -*-
import sys
import pickle
import pandas as pd
import time
import datetime
import argparse
from apscheduler.schedulers.background import BackgroundScheduler

import crawler
from crawler.comp_fnguide import metric_crawler
from crawler.kind_krx import krx_crawler
from crawler.finance_naver import naver_report_crawler, naver_price_crawler
from screener import screener
from dev_util.util import mysql_manager, timer, logger, config

#################
# 1. 매일 KRX에서 종목명/코드 수집 : krx_crawler
# 2. 종목별 주가 수집
# 3. 종목별 fnguide 정보 수집 : metric_crawler
# 4. 네이버 종목 분석 게시판 수집(애널리스트 리포트)
# 5. 네이버에서 테마 정보 수집
# 6. 네이버 종목 게시판의 게시글 조회수 수집(관심도)
# 7. 학습한 모델로 inference후 저장
#################
_logger = logger.APP_LOGGER

def crawl_krxcode_daily():
    '''KRX에서 종목 코드 가져오기'''
    krx = krx_crawler.KRXCrawler()
    krx.crawl(save=True)
    _logger.debug(f'crawl_krxcode_daily job done')

def crawl_analyst_report_daily():
    '''네이버 종목 분석 게시판 수집(애널리스트 리포트)'''
    # while True:
    nreport = naver_report_crawler.NaverReportCrawler()
    nreport.crawl(save=True)
    _logger.debug(f'crawl_naver_report_daily job done')

    # next day
    config.BASIS_DATE = timer.add_date(config.BASIS_DATE, 1)

def crawl_price_daily():
    '''yahoo finance에서 일자별 가격정보 가져오기'''
    drc = naver_price_crawler.NaverPriceCrawler()
    drc.crawl(save=True)
    _logger.debug(f'crawl_krxcode_daily job done')

    # infer
    infer_model_daily()

def crawl_metric_daily():
    '''fnguide에서 재무제표 가져오기'''
    sp = metric_crawler.MetricCrawler()
    sp.crawl(save=True)
    _logger.debug(f'crawl_metric_daily job done')

    # infer
    infer_model_daily()

def infer_model_daily():
    '''model inference후 저장'''
    scr = screener.Screener()
    scr.recommend(save=True)
    _logger.debug(f'infer_model_daily job done')

def update_date():
    config.BASIS_DATE = timer.get_now('%Y-%m-%d')

# scheduler
def scheduler():
    period = config.CONFIG.PERIOD
    sched = BackgroundScheduler({'apscheduler.timezone': 'Asia/Seoul'})
    sched.add_job(update_date, 'cron', day_of_week=period.update_date.day_of_week, hour=period.update_date.hour)
    sched.add_job(crawl_krxcode_daily, 'cron', day_of_week=period.krx_crawler.day_of_week, hour=period.krx_crawler.hour)
    sched.add_job(crawl_price_daily, 'cron', day_of_week=period.price_crawler.day_of_week, hour=period.price_crawler.hour)
    sched.add_job(crawl_metric_daily, 'cron', day_of_week=period.metric_crawler.day_of_week, hour=period.metric_crawler.hour)
    sched.add_job(crawl_analyst_report_daily, 'cron', day_of_week=period.report_crawler.day_of_week, hour=period.report_crawler.hour)
    # sched.add_job(infer_model_daily, 'cron', day_of_week=period.model_infer.day_of_week, hour=period.model_infer.hour)
    # sched.add_job(scheduler.crawl_daily_inout, 'cron', day_of_week=weekday, hour='9-15', minute='0-59/30')
    sched.start()

def unit_test():
    update_date()
    crawl_krxcode_daily() # 1
    crawl_price_daily() # 2
    crawl_metric_daily() # 3
    crawl_analyst_report_daily() # 4
    infer_model_daily()
    pass

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--run_type', required=True, choices=['real', 'test'], help='실행모드 입니다.')
    args = parser.parse_args()

    config.load_config(run_type=args.run_type) # test, real
    print('Basis date :', config.BASIS_DATE)
    # config.CONFIG.pprint(pformat='json')

    try:
        print('start!')
        # unit_test()
        scheduler()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        # Ctrl+C 입력시 예외 발생
        sys.exit() #종료
