# -*- coding: utf-8 -*-

import sys
import pandas as pd
import pymysql
from sqlalchemy.exc import IntegrityError
from sqlalchemy import create_engine
from util import logger, config


class MysqlController:

    def __init__(self):
        self.logger = logger.APP_LOGGER

        # MySQL Connection 연결
        db_config = config.CONFIG.MYSQL_CONFIG
        self.conn = pymysql.connect(host=db_config.MYSQL_HOST,
                                    user=db_config.MYSQL_USER,
                                    password=db_config.MYSQL_PASSWD,
                                    db=db_config.MYSQL_DB,
                                    charset='utf8')
        self.curs = self.conn.cursor(pymysql.cursors.DictCursor)
        self.engine = create_engine(
                        '''mysql+pymysql://{user}:{passwd}@{svr}/{db_name}?charset=utf8'''.format(
                            user=db_config.MYSQL_USER,
                            passwd=db_config.MYSQL_PASSWD,
                            svr=db_config.MYSQL_HOST,
                            db_name=db_config.MYSQL_DB),
                        encoding='utf8')
    def __del__(self):
        if self.conn is not None:
            self.conn.close()
        if self.curs is not None:
            self.curs.close()

    def select(self, query):
        # ==== select example ====
        self.curs.execute(query)

        # 데이타 Fetch
        rows = self.curs.fetchall()
        return rows

    def insert(self, query):
        # ==== insert example ====
        # sql = """insert into customer(name,category,region)
        #         values (%s, %s, %s)"""
        # self.curs.execute(sql, ('이연수', 2, '서울'))
        self.curs.execute(query)
        self.conn.commit()

    def update(self, query):
        self.curs.execute(query)
        self.conn.commit()

    def delete(self, query):
        self.update(query)

    def select_dataframe(self, query):
        self.logger.debug(f'Select Datarame : {query}')
        df = pd.read_sql(query, self.engine)
        return df

    def insert_dataframe(self, df, table, index=False, ignore_duplicate=True):
        try:
            df.to_sql(name=table, con=self.engine, index=index, if_exists='append')
            self.logger.debug(f'Insert Datarame into {table} : {len(df)}')
        except IntegrityError as e:
            for i in range(len(df)):
                row = df.iloc[[i]]
                try:
                    row.to_sql(name=table, con=self.engine, index=index, if_exists='append')
                except IntegrityError as e:
                    if not ignore_duplicate:
                        self.logger.debug(f'Duplicated Row ({table}) : {e.args[0]}')
                except Exception as e:
                    self.logger.error(f'Insert(In duplicated) Datarame Error ({table}) : {e}')
        except Exception as e:
            self.logger.error(f'Insert Datarame Error ({table}) : {e}')

