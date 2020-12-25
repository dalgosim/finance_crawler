# -*- coding: utf-8 -*-
import sys
import pickle
import pandas as pd
import time
import datetime
import argparse
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

import crawler
from crawler.comp_fnguide import metric_crawler
from crawler.kind_krx import krx_crawler
from crawler.finance_naver import naver_report_crawler, naver_price_crawler, recommendation_item_cralwer
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
    _logger.info(f'crawl_krxcode_daily job done')

def crawl_analyst_report_daily():
    '''네이버 종목 분석 게시판 수집(애널리스트 리포트)'''
    nreport = naver_report_crawler.NaverReportCrawler()
    nreport.crawl(save=True)
    _logger.info(f'crawl_naver_report_daily job done')

def crawl_recommendation_item_daily():
    '''네이버 종목추천 게시판 수집(지금은 사라짐)'''
    nrecom = recommendation_item_cralwer.RecommendationItemCrawler()
    nrecom.crawl(proc=1, save=True)
    nrecom.crawl(proc=2, save=True)
    _logger.debug(f'crawl_naver_recom_daily job done')

def crawl_price_daily():
    '''naver finance에서 일자별 가격정보 가져오기'''
    drc = naver_price_crawler.NaverPriceCrawler()
    drc.crawl(save=True)
    _logger.info(f'crawl_price_daily job done')

    # infer
    infer_model_daily()

def crawl_metric_daily():
    '''fnguide에서 재무제표 가져오기'''
    sp = metric_crawler.MetricCrawler()
    sp.crawl(save=True)
    _logger.info(f'crawl_metric_daily job done')

    # infer
    infer_model_daily()

def infer_model_daily():
    '''model inference후 저장'''
    scr = screener.Screener()
    scr.recommend(save=True)
    _logger.info(f'infer_model_daily job done')

def update_date():
    config.BASIS_DATE = timer.get_now('%Y-%m-%d')
    _logger.info('======================================')
    _logger.info(f'current basis_date : {config.BASIS_DATE}')
    _logger.info('======================================')

# scheduler
def scheduler():
    period = config.CONFIG.PERIOD
    sched = BackgroundScheduler({'apscheduler.timezone': 'Asia/Seoul'})
    sched.add_job(update_date, 'cron', day_of_week=period.update_date.day_of_week, hour=period.update_date.hour)
    sched.add_job(crawl_krxcode_daily, 'cron', day_of_week=period.krx_crawler.day_of_week, hour=period.krx_crawler.hour)
    sched.add_job(crawl_price_daily, 'cron', day_of_week=period.price_crawler.day_of_week, hour=period.price_crawler.hour)
    sched.add_job(crawl_metric_daily, 'cron', day_of_week=period.metric_crawler.day_of_week, hour=period.metric_crawler.hour)
    sched.add_job(crawl_analyst_report_daily, 'cron', day_of_week=period.report_crawler.day_of_week, hour=period.report_crawler.hour)
    sched.add_job(crawl_recommendation_item_daily, 'cron', day_of_week=period.recom_inout_crawler.day_of_week, hour=period.recom_inout_crawler.hour)
#     sched.add_job(infer_model_daily, 'cron', day_of_week=period.model_infer.day_of_week, hour=period.model_infer.hour)
    # sched.add_job(scheduler.crawl_daily_inout, 'cron', day_of_week=weekday, hour='9-15', minute='0-59/30')
    sched.start()
    _logger.debug('scheduler start!')

def unit_test():
#     update_date()
    config.BASIS_DATE = "2020-11-25"
#     crawl_krxcode_daily() # 1
#     crawl_price_daily() # 2
#     crawl_metric_daily() # 3
    crawl_analyst_report_daily() # 4
#     infer_model_daily()
    pass



# for background
app = Flask(__name__)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--run_type', default='real', choices=['real', 'test'], help='실행모드 입니다.')
    args = parser.parse_args()

    config.load_config(run_type=args.run_type) # test, real
    print('Basis date :', config.BASIS_DATE)
    # config.CONFIG.pprint(pformat='json')

#     unit_test()
    scheduler()
    app.run()
