# -*- coding: utf-8 -*-

from util import logger
from util import config, mysql_manager

class Crawler:
    
    def __init__(self, delay=1):
        self.logger = logger.APP_LOGGER
        self.delay = delay

    def crawl(self):
        pass

    def save(self):
        pass

def get_company_list():
    _mysql = mysql_manager.MysqlController()
    query = f'''SELECT distinct cmp_cd FROM {config.CONFIG.DATABASE.COMPANY_LIST_TABLE};'''
    return _mysql.select_dataframe(query)
