# -*- coding: utf-8 -*-
import json
from dotmap import DotMap

from util import timer

CONFIG = DotMap()
BASIS_DATE = timer.get_yesterday('%Y-%m-%d')

def load_config(run_type='test', path='config.json'):
    global CONFIG

    AUTH_PATH = './auth/mysql_auth.json'

    # load scheduler config
    with open(path) as f:
        config_data = json.load(f)
    
    # load database config
    with open(AUTH_PATH) as f:
        auth_data = json.load(f)

        # 사용하는 설정만 남기고 지우기
        if run_type.lower() == 'real':
            auth_data['MYSQL_CONFIG'] = auth_data['MYSQL_SVR']['REAL_DB']
        else:
            auth_data['MYSQL_CONFIG'] = auth_data['MYSQL_SVR']['DEV_DB']
        auth_data.pop('MYSQL_SVR')

    # merge config
    merged_dict = {**config_data, **auth_data}
    CONFIG = DotMap(merged_dict)
