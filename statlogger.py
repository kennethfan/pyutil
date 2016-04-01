#!/usr/bin/python
# -*- coding: utf-8 -*-
# Date: 2016-04-01

from time import strftime
import time
import json
from traceback import format_exc

class StatLogger():
    """StatLog Util"""
    def __init__(self, config):
        logfile = config['file']
        self._f = open(logfile, 'a+')
        self._levels = config['levels']
        self._format = '%Y%m%d %H:%M:%S'
        if 'format' in config:
            self._format = config['format']

    def __del__(self):
        self._f.close()

    def stat(self, op, fields={}, level='info'):
        if level not in self._levels:
            return
        timestr = strftime(self._format)
        fieldsstr = ' '.join(['%s=%s' % (k,str(v).replace(' ', '_') )
                             for k, v in fields.items() ])
        self._f.write( '%s [%s] op=%s %s\n' % (timestr, level, op, fieldsstr) )
        self._f.flush()

    def log(self, message, level='info'):
        if level not in self._levels:
            return
        timestr = strftime(self._format)
        self._f.write( '%s [%s] %s\n' % (timestr, level, message) )
        self._f.flush()

    def json(self, op, msg, level='info'):
        if level not in self._levels:
            return
        json_msg = msg.copy()
        json_msg['op'] = op
        json_msg['_log_time'] = int(time.time())
        self._f.write('%s\n' %json.dumps(json_msg))
        self._f.flush()


    def exception(self, op=None, fields={}):
        if 'exception' not in self._levels:
            return
        if op is not None:
            self.stat(op, fields, 'exception')
        self._f.write(format_exc())
        self._f.flush()


def __test_log():
    logger = StatLogger({'file':'/tmp/test_log', 'levels':('info', 'error')})
    logger.stat(
        'send_mail_succ', {'reciver':'hello', 'email':'hello@world.com'})
    logger.log(
        'send_mail_fail','error')
    logger.log(
        'send_mail_fail', 'trace')
    try:
        raise Exception('test exception')
    except Exception:
        logger.exception('unhandled_exception')


if __name__ == '__main__':
    __test_log()
