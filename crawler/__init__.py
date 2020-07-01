# -*- coding: utf-8 -*-
from dev_util.util import logger, config, mysql_manager


class Crawler:
    
    def __init__(self):
        self.logger = logger.APP_LOGGER
        self.delay = config.CONFIG.CRAWL_DELAY
        self.basis_date = config.BASIS_DATE
        self.mysql = mysql_manager.MysqlController()

    def crawl(self, save=False):
        pass

    def save(self):
        pass

