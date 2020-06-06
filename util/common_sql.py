# -*- coding: utf-8 -*-
from util import config, mysql_manager


def get_company_list(limit=0):
    _mysql = mysql_manager.MysqlController()
    table = config.CONFIG.MYSQL_CONFIG.TABLES.COMPANY_LIST_TABLE
    limit_q = f'''LIMIT {limit}''' if limit > 0 else ''
    query = f'''SELECT distinct cmp_cd FROM {table} {limit_q};'''
    return _mysql.select_dataframe(query)
