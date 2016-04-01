#!/usr/bin/python
# -*- coding: utf-8 -*-
# Desc: 运行时debug工具
# Date: 2016-04-01


import StringIO
import traceback


class DebugUtil:
    @staticmethod
    def get_debug_info():
        '''
            @desc 获取debug信息
            @return string
        '''
        fp = StringIO.StringIO()
        traceback.print_exc(file=fp)
        return fp.getvalue()


def __test_debug_info():
    print DebugUtil.get_debug_info()


if '__main__' == __name__:
    __test_debug_info()
