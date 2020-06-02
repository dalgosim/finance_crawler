# -*- coding: utf-8 -*-
import json
from dotmap import DotMap

CONFIG = DotMap()

def load_config(run_type='test', path='config.json'):
    global CONFIG

    AUTH_PATH = './auth/mysql_auth.json'

    # load scheduler config
    with open(path) as f:
        jdata = json.load(f)
        CONFIG = DotMap(jdata)
    
    # load database config
    with open(AUTH_PATH) as f:
        jdata = json.load(f)
        CONFIG = DotMap(jdata)
        CONFIG.MYSQL_SVR = CONFIG.REAL_DB if run_type.lower() == 'real' else CONFIG.DEV_DB
        CONFIG.pop('REAL_DB')
        CONFIG.pop('DEV_DB')
