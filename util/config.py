# -*- coding: utf-8 -*-
import json
from dotmap import DotMap

from util import timer, logger

_logger = logger.APP_LOGGER
CONFIG = DotMap()
BASIS_DATE = timer.get_now('%Y-%m-%d')
TEST_MODE = True

def load_config(run_type='test', path='config.json'):
    global CONFIG, TEST_MODE, BASIS_DATE

    AUTH_PATH = './auth/mysql_auth.json'

    # set date
    if timer.get_now_hour() < 16: #4시 이전에는 전날 기준으로 가져오기
        BASIS_DATE = timer.get_yesterday('%Y-%m-%d')

    # load scheduler config
    with open(path) as f:
        config_data = json.load(f)
    
    # load database config
    with open(AUTH_PATH) as f:
        auth_data = json.load(f)

        # 사용하는 설정만 남기고 지우기
        TEST_MODE = False if run_type.lower() == 'real' else True
        _logger.debug(f'===== TEST_MODE : {TEST_MODE} =====')

        if TEST_MODE:
            auth_data['MYSQL_CONFIG'] = auth_data['MYSQL_SVR']['DEV_DB']
        else:
            auth_data['MYSQL_CONFIG'] = auth_data['MYSQL_SVR']['REAL_DB']
        auth_data.pop('MYSQL_SVR')

    # merge config
    merged_dict = {**config_data, **auth_data}
    CONFIG = DotMap(merged_dict)
