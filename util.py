#!/usr/local/bin/python2.7
# -*- coding: utf-8 -*-
# Date: 2016-04-01

import threading
import ConfigParser
import redis
import time
import beanstalkc
import pymongo
import types
import os
import json
import itertools
import dbutil
from daemon import Daemon
from statlogger import StatLogger
from apscheduler.scheduler import Scheduler
from datetime import datetime


class Util(object):
    components = threading.local()

    @classmethod
    def init(cls, config):
        # init Logger
        if 'log' in config:
            config['log']['file'] = time.strftime(config['log']['file'])
            config['log']['levels'] = [c.lower() for c in
                                       config['log']['level'].split(',')]
            cls.logger = StatLogger(config['log'])
        # init redis connection
        if 'redis' in config:
            redisconfig = config['redis']
            cls.redis = redis.StrictRedis(redisconfig['host'],
                                          int(redisconfig['port']))
        if 'beanstalk' in config:
            beanstkconfig = config['beanstalk']
            cls.beanstalk = beanstalkc.Connection(beanstkconfig['host'],
                                                  int(beanstkconfig['port']))
        if 'mongodb' in config:
            mongodbconfig = config['mongodb']
            cls.mongodb = pymongo.MongoClient(mongodbconfig['host'],
                                              int(mongodbconfig['port']))

    @classmethod
    def get_components(cls):
        return cls.components

    @classmethod
    def get_mongodb(cls):
        return cls.mongodb

    @classmethod
    def get_redis(cls):
        return cls.redis

    @classmethod
    def get_logger(cls):
        return cls.logger

    @classmethod
    def get_beanstalk(cls):
        return cls.beanstalk

    @classmethod
    def load_config(cls, config_file='config.ini'):
        cfg = ConfigParser.ConfigParser()
        config_info = {}
        try:
            cfg.read(config_file)
            for section in cfg.sections():
                section_items = {}
                for key in cfg.options(section):
                    section_items[key] = cfg.get(section, key)
                config_info[section] = section_items
        except Exception as err:
            print "解析配置错误:", err
            raise err
        return config_info

    @classmethod
    def debug(cls, str, vars={}):
        print "%s:%s params:%s" % (
            time.strftime('%Y/%m/%d %H:%M:%S'),
            str, ' '.join(['%s=%s' % (a, b) for a, b in vars.items()]))


def safeunicode(obj, encoding='utf-8'):
    r"""
    Converts any given object to unicode string.

        >>> safeunicode('hello')
        u'hello'
        >>> safeunicode(2)
        u'2'
        >>> safeunicode('\xe1\x88\xb4')
        u'\u1234'
    """
    t = type(obj)
    if t is unicode:
        return obj
    elif t is str:
        return obj.decode(encoding)
    elif t in [int, float, bool]:
        return unicode(obj)
    elif hasattr(obj, '__unicode__') or isinstance(obj, unicode):
        return unicode(obj)
    else:
        return str(obj).decode(encoding)


def safestr(obj, encoding='utf-8'):
    r"""
    Converts any given object to string.

        >>> safestr(u'hello')
        'hello'
        >>> safeunicode(2)
        '2'
    """
    if isinstance(obj, unicode):
        return obj.encode(encoding)
    elif isinstance(obj, str):
        return obj
    elif hasattr(obj, 'next'):  # iterator
        return itertools.imap(safestr, obj)
    elif obj is None:
        return ''
    else:
        return str(obj)

# 对输出日志格式化


def format_log(log_op, log_info):
    field_list = ["op=" + log_op]

    if log_info:
        for key, value in log_info.items():
            field_list.append(safestr(key) + "=" + safestr(value))

    return " ".join(field_list)


class JobScheduler(Daemon):
    '''
    任务调度类
    具体例子可见 /var/www/beva/server/erp/erp_sched.py
    Usage:
        job_list = [
            {'module_name': 'your_module_name',
             'function_name': 'your_function_name',
             #调度类型:cron类型
             'sched_type': 'cron',
             #调度参数,例子中表示每天7点钟执行
             #详见https://apscheduler.readthedocs.org/en/latest/cronschedule.html
             'kwargs': {'hour': 7}},
            {'module_name': 'your_module_name_2',
             'function_name': 'your_function_name_2',
             #调度类型:'interval'
             'sched_type': 'interval',
             #调度参数,表示每隔五分钟就会运行一次
             #详见https://apscheduler.readthedocs.org/en/latest/cronschedule.html
             'kwargs': {'minutes': 5}}]

        #配置日志
        basic_logger.get_logger(
            file_name='your_log_file_name',
            #logger_name,schedule的logger_name
            logger_name='apscheduler.scheduler',
            path='/path/to/log_file(direcotry only)',
            #当出现错误日志时,会将日志发送到邮箱
            mail_list=['your_mails',...])

        test = JobScheduler(
            pidfile='/tmp/test_scheduler.pid',
            stdout='/path/to/stdout_file',
            stderr='/path/to/stderr_file',
            job_list=job_list)
        if len(sys.argv) == 2:
            if 'start' == sys.argv[1]:
                test.start()
            elif 'stop' == sys.argv[1]:
                test.stop()
            elif 'restart' == sys.argv[1]:
                test.restart()
            else:
                print 'Unknown command'
                sys.exit(1)
        else:
            print 'Usage %s start|stop|restart' % sys.argv[0]
    '''

    def __init__(self, **kwargs):
        self.sched = Scheduler(standalone=True)
        self.job_list = kwargs.get('job_list', [])
        super(JobScheduler, self).__init__(
            pidfile=kwargs['pidfile'],
            stdout=kwargs['stdout'],
            stderr=kwargs['stderr'])

    def add_sched_jobs(self, kwargs):
        function_name = 'add_' + kwargs['sched_type'] + '_job'
        module = __import__(
            kwargs['module_name'], fromlist=[kwargs['function_name']])
        getattr(self.sched, function_name)(
            getattr(module, kwargs['function_name']),
            **kwargs['kwargs'])

    def run(self):
        for job_config in self.job_list:
            self.add_sched_jobs(job_config)
        self.sched.start()
        self.sched.shutdown()


def config_parser(cfg_file):
    import ConfigParser
    parser = ConfigParser.ConfigParser()
    parser.read(cfg_file)
    conf_dict = {}
    for section in parser.sections():
        item = {}
        for option in parser.options(section):
            item[option] = parser.get(section, option)
        conf_dict[section] = item
    return conf_dict


def sql_decorator(func):
    '''执行sql之前先在日志中打印执行的sql'''
    def wrapped(self, *args, **kwargs):
        kwargs['_test'] = True
        msg = func(self, *args, **kwargs)
        del kwargs['_test']
        self.logger.log('execute_sql=%s' % msg, 'debug')
        return func(self, *args, **kwargs)
    return wrapped


class EmptyConfigError(Exception):
    pass


def convert_data(data, encode='utf-8'):
    '''
    将unicode 转换为 str,适用于各种数据结构
    '''
    if isinstance(data, dict):
        return dict(map(convert_data, data.items()))
    elif isinstance(data, tuple):
        return tuple(map(convert_data, data))
    elif isinstance(data, list):
        return map(convert_data, data)
    else:
        return safestr(data)


class Base(object):
    '''
    基类
    Usage:
        class Test(Base):
            def __init__(self, cfgs):
                #需要设置path属性,方便查找配置文件的日志文件
                self.path = os.path.abspath(os.path.dirname(__file__))
                super(Test, self).__init__(cfgs)
            def run():
                process ...
    '''

    def __init__(self, cfg_files):
        self.config = {}
        if not cfg_files:
            raise EmptyConfigError('empty config files!')
        if not isinstance(cfg_files, list):
            cfg_files = [cfg_files]
        for cfg_file in cfg_files:
            if not cfg_file.startswith('/'):
                cfg_file = os.path.join(self.path, 'config', cfg_file)
            self.config.update(config_parser(cfg_file))
        if 'log' in self.config:
            if 'cut_log' not in self.config['log']:
                self.config['log']['file'] += '.' + datetime.strftime(
                    datetime.now(),
                    '%Y%m%d')
            self.config['log']['levels'] = [
                c.lower() for c in self.config['log']['level'].split(',')]
            self.logger = StatLogger(self.config['log'])
        if 'redis' in self.config:
            import redis
            redisconfig = self.config['redis']
            self.redis = redis.StrictRedis(
                redisconfig['host'], int(redisconfig['port']))
        if 'beanstalk' in self.config:
            import beanstalkc
            beanstkconfig = self.config['beanstalk']
            self.beanstalkc = beanstalkc.Connection(
                beanstkconfig['host'], int(beanstkconfig['port']))
        if 'mongodb' in self.config:
            import pymongo
            mongodbconfig = self.config['mongodb']
            self.mongodb = pymongo.MongoClient(
                mongodbconfig['host'], int(mongodbconfig['port']))

    def _get_beanstalkc(self, tube=None, oper='export'):
        try:
            self.beanstalkc.close()
        except:
            pass
        finally:
            self.beanstalkc.connect()
        if not tube:
            return
        if oper == 'export':
            self._watch_single_tube(tube)
        elif oper == 'import':
            self.use(tube)

    def _watch_single_tube(self, tube):
        self.beanstalkc.watch(tube)
        tubes = self.beanstalkc.watching()
        for watch_tube in tubes:
            if watch_tube != tube:
                self.beanstalkc.ignore(watch_tube)

    def export_queue(self, tube, json_data=True, timeout=0):
        '''
        从beanstalk中取出数据
        参数:
            tube: tube名称
            json_date: 默认为True, 将取出的数据转化成字典,否则原样返回
            timeout: reserve的超时时间,默认为0
        返回值: export_queue是一个generator function,返回iterator
        Usage:
            export_queue是一个generator function,返回iterator
            for data in self.export_queue(self, tube_name):
                process data here
        '''
        try:
            self._watch_single_tube(tube)
        except:
            self._get_beanstalkc(tube, oper='export')
        while True:
            try:
                if timeout is not None:
                    job = self.beanstalkc.reserve(timeout=timeout)
                else:
                    job = self.beanstalkc.reserve()
            except:
                self._get_beanstalkc(tube, oper='export')
                if timeout is not None:
                    job = self.beanstalkc.reserve(timeout=timeout)
                else:
                    job = self.beanstalkc.reserve()
            if not job:
                break
            if json_data:
                try:
                    data = json.loads(job.body)
                except:
                    job.bury()
                    yield {}
                else:
                    yield data
                    job.delete()
            else:
                data = job.body
                yield data
                job.delete()

    def import_queue(self, tube, data, json_data=True):
        '''
        向beanstalk中导入数据
        参数:
            tube: tube名称
            data: 待放入队列的数据
            json_data: 是否进行dumps,默认为True
        返回值:导入队列是否成功
        '''
        try:
            self.beanstalkc.use(tube)
        except:
            self._get_beanstalkc(tube, oper='import')
        try:
            if json_data:
                data = json.dumps(data)
            self.beanstalkc.put(data)
        except Exception as err:
            self.logger.log('import_data_error:%s' % err)
            return False
        else:
            return True

    def get_db(self, db):
        '''
        得到数据库连接
        参数:
            db: 数据库名称,如crm,在ini文件中对应crmDb的条目
        返回值: 数据库连接
        '''
        if not db.startswith('db_'):
            db = 'db_' + db
        if db.endswith('Db'):
            db = db[:-2]
        db_title = db[3:]
        if getattr(self, db_title, None):
            return getattr(self, db_title)
        else:
            db_config = self.config[db_title + 'Db']
            conn = dbutil.database(
                dbn='mysql',
                    host=db_config['host'],
                    port=int(db_config['port']),
                    user=db_config['username'],
                    passwd=db_config['password'],
                    db=db_config['database'],
                    charset=db_config.get('charset', 'utf8')
            )
            setattr(self, db_title, conn)
            return getattr(self, db_title)

    @sql_decorator
    def query(self, db, *args, **kwargs):
        '''
        执行查询操作,返回iterator
        '''
        conn = self.get_db(db)
        return conn.query(*args, **kwargs)

    @sql_decorator
    def insert(self, db, *args, **kwargs):
        '''
        执行插入操作,返回新增记录的id
        '''
        conn = self.get_db(db)
        return conn.insert(*args, **kwargs)

    @sql_decorator
    def update(self, db, *args, **kwargs):
        '''
        执行更新操作,返回更新的条目数
        '''
        conn = self.get_db(db)
        return conn.update(*args, **kwargs)
