# -*- coding: utf-8 -*-
import json
from dotmap import DotMap

CONFIG = DotMap()

def load_config(run_type='test', path='config.json'):
    global CONFIG

    with open(path) as f:
        jdata = json.load(f)
        CONFIG = DotMap(jdata)
        CONFIG.DATABASE = CONFIG.REAL_DB if run_type.lower() == 'real' else CONFIG.DEV_DB