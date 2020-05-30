# -*- coding: utf-8 -*-
import json
from dotmap import DotMap

CONFIG = DotMap()

def load_config(path='config.json'):
    global CONFIG

    with open(path) as f:
        jdata = json.load(f)
        CONFIG = DotMap(jdata)
