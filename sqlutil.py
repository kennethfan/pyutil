#!/usr/bin/python
# -*- coding: utf-8 -*-
# Date: 2016-04-01

import pymysql
from util import Util

class SqlUtil(object):
    """
    mysql 连接工具
    """
    @staticmethod
    def get_db(dbconfig, shareconn=False):
        try:
            charset = dbconfig.get('charset') or 'utf8'
            conn = pymysql.connect(
                    dbconfig['host'], dbconfig['username'],
                    dbconfig['password'], dbconfig.get('database'),
                    int(dbconfig['port']), charset=charset)
            return SqlConn(conn, shareconn)
        except Exception as err:
            Util.get_logger().exception(op='sql_exception', fields=dbconfig)
            raise err

    @staticmethod
    def query(dbconfig, sql, shareconn=False, autocommit=False):
        Conn = SqlUtil.get_db(dbconfig, shareconn)
        return  Conn.query_sql(sql, autocommit)

    @staticmethod
    def insert(dbconfig, table, pairs, shareconn=False, autocommit=False):
        sql = SqlUtil.create_insert_sql(table, pairs)
        Conn = SqlUtil.get_db(dbconfig, shareconn)
        return Conn.query_sql(sql, autocommit)

    @staticmethod
    def create_insert_sql(table, pair):
        key_str = ",".join(["`%s`" % key for key in pair.keys()])
        value_str = ",".join(["{%s}" % key for key in pair.keys()])
        sql = r"insert into %s (%s) values (%s)" % (table, key_str, value_str)
        temp_data = {}
        for key, value in pair.items():
            temp_data[key] = SqlUtil.safestr(pymysql.escape_string(str(value)), 'utf-8')
        return sql.format(**temp_data)

    @staticmethod
    def format_column(column):
        if isinstance(column ,list) or isinstance(column, tuple):
            column = ','.join(['`%s`' % x for x in column])
        if isinstance(column , dict):
            column = ','.join(['`%s` as `%s`' % x for x in column.items()])
        return column

    @staticmethod
    def safestr(obj, encoding='utf-8'):
        if isinstance(obj, unicode):
            return obj.encode(encoding)
        elif isinstance(obj, str):
            return obj
        elif hasattr(obj, 'next'): # iterator
            return itertools.imap(safestr, obj)
        elif obj is None:
            return ''
        else:
            return str(obj)


class SqlConn(object):
    """
    mysql 连接
    """
    def __init__(self, conn, share):
        self._conn = conn
        self.__share = share
        self.__db = None

    def __del__(self):
        if not self.__share:
            self._conn.close()

    def __getattr__(self, name):
        return Querier(self, name)

    def query_sql(self, sql, autocommit=False):
        try:
            cur = self._conn.cursor(pymysql.cursors.DictCursor)
            cur.execute(sql)
            result = cur.fetchall()
            if autocommit:
                self.commit()
            if sql.find('insert into ') != -1:
                return cur.lastrowid
            if sql.find('update ') != -1:
                return
            data = []
            for record in result:
                r = {}
                for k, v in record.items():
                    r[SqlUtil.safestr(k)] = SqlUtil.safestr(v)
                data.append(r)
            return data
        except Exception as e:
            Util.get_logger().log('query sql error: %s' % e)
            raise e

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def query(self, *args):
        if args['method'] == 'select':
            column = format_column(column)
            where =  format_where(where)
            orderby = format_orderby(orderby)
            sql = 'select %s from %s' % (column, self.__table)


class Querier(object):
    def __init__(self, conn, name):
        self.__conn  = conn
        self.__table = name

    def __getattr__(self, name):
        if name in ('select', 'insert', 'update', 'delete'):
            return Method(self.__conn, self.__table, name)
        else:
            self.__table = '%s.%s' %(self.__table, name)
            return self

class Method(object):
    def __init__(self, conn, table, method):
        self.__conn = conn
        self.__table = table
        self.__method = method

    def __call__(self, *args):#column='*', where=None, orderby=None, limit=None):
        args['table'] = self.__table
        args['method'] = self.__method
        return self.__conn.query(*args)


def __test_query():
    pass
    #pair = {'nickname':'__WWWWW\'\'\''}
    #pair1 = {'nickname':'helloworld'}
    #config = {'host':'localhost', 'username':'user', 'password':'passwd',
    #          'port':'3306', 'database':'db_test'}
    #r = SqlUtil.query(config, 'select id, nickname from t_user where id < 10010')
    #print len(r)
    #for record in r:
    #    print '    '.join(['%s=>%s' % x for x in record.items()])
    #sql = SqlUtil.create_insert_sql('t_user',pair)
    #sql1 = SqlUtil.create_insert_sql('t_user',pair1)
    #print sql
    #print sql1


if __name__ == '__main__':
    __test_query()

