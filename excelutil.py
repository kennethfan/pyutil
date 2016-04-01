#!/usr/local/python/bin/python
# -*- coding: UTF-8 -*-
# Desc: python操作excel工具
# Date: 2016-04-01

import xlrd
import traceback
import StringIO
import os


class ExcelReadUtil:
    '''
        Excel 读操作工具
        目前仅支持2003格式
    '''
    def __init__(self, filename):
        if not os.path.exists(filename):
            print '%s not exists' % filename
            sys.exit()
        try:
            self.workbook = xlrd.open_workbook(filename)
            self.set_work_sheet(0)
        except:
            fp = StringIO.StringIO()
            traceback.print_exc(file=fp)
            print fp.getvalue()
            sys.exit()

    def set_work_sheet(self, index):
        '''
            @desc 设置当前的工作表
        '''
        if index >= len(self.workbook.sheets()):
            print '%d out of the range of excel sheets' % index
            sys.exit()
        self.sheet = self.workbook.sheets()[index]

    def get_data_row_num(self):
        '''
            @desc 获取工作表行数
            @return integer
        '''
        return self.sheet.nrows

    def get_data_col_num(self):
        '''
            @desc 获取工作表列数
            @return integer
        '''
        return self.sheet.ncols

    def get_data_row_values(self, row_index):
        '''
            @desc 获取行数据
            @param int row_index
            @return list
        '''
        if not self.check_data_row(row_index):
            print '%d out of the range of sheet rows' % row_index
            sys.exit()
        return self.sheet.row_values(row_index)

    def get_data_col_values(self, col_index):
        '''
            @获取列数据
            @param int col_index
            @return list
        '''
        if not self.check_data_col(col_index):
            print '%d out of the range of sheet cols' % col_index
            sys.exit()
        return self.sheet.col_values(col_index)

    def get_data_cell_value(self, row_index, col_index):
        '''
            @desc 获取单元格数据
            @param int row_index
            @param int col_index
            @return string
        '''
        if not self.check_data_row(row_index):
            print '%d out of the range of sheet rows' % row_index
            sys.exit()
        if not self.check_data_col(col_index):
            print '%d out of the range of sheet cols' % col_index
            sys.exit()
        return self.sheet.cell(row_index, col_index).value

    def check_data_row(self, row_index):
        '''
            @desc 检测行值合法性
            @param int row_index
            @return boolean
        '''
        if row_index >= self.get_data_row_num():
            return False
        return True

    def check_data_col(self, col_index):
        '''
            @desc 检测列合法性
            @param int col_index
            @return boolean
        '''
        if col_index >= self.get_data_col_num():
            return False
        return True
