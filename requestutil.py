#!/usr/bin/python
# -*- coding: utf-8 -*-
# Desc http请求工具
# Date: 2016-04-01

import requests


class RequestUtil:
    @staticmethod
    def attach_referer(headers, referer=''):
        '''
            @desc 指定referer
            @param dict headers http请求头
            @param string referer
        '''
        if referer:
            headers['Referer'] = referer

    @staticmethod
    def create_request_proxy_data(proxy):
        '''
            @desc 创建代理数据
            @return dict
        '''
        try:
            proxy['ip'] = proxy['ip'].encode('utf-8')
        except:
            pass
        try:
            proxy['port'] = proxy['port'].encode('utf-8')
        except:
            pass
        return {'http': r'%s:%s' % (proxy['ip'], proxy['port'])}

    @classmethod
    def create_request_options(cls, **kw):
        '''
            @desc 创建ruquest请求选项
        '''
        headers = {}
        options = {}
        if 'user-agent' in kw:
            headers['User-Agent'] = kw['user-agent']
        if 'referer' in kw:
            cls.attach_referer(headers, kw['referer'])
        if 'proxy' in kw:
            options['proxies'] = cls.create_request_proxy_data(kw['proxy'])
        if 'cookie' in kw:
            options['cookies'] = kw['cookie']
        if 'timeout' in kw:
            options['timeout'] = float(kw['timeout'])
        if 'data' in kw:
            options['data'] = kw['data']
        if 'stream' in kw and kw['stream']:
            options['stream'] = True
        if headers:
            options['headers'] = headers
        return options

    @classmethod
    def update_post_data(cls, options, **kw):
        if 'post_params' not in kw:
            return
        post_params = {}
        for code in kw['post_params']:
            exec(code)
        if not post_params:
            return
        if 'data' not in options:
            options['data'] = {}
        options['data'].update(post_params)

    @classmethod
    def attach_url_get_params(cls, url, get_params_code):
        get_params = {}
        for code in get_params_code:
            exec(code)
        if not get_params:
            return url
        kv_list = []
        for k, v in get_params.items():
            kv_list.append('%s=%s' % (k, str(v)))
        if '?' in url:
            separator = '&'
        else:
            separator = '?'
        return url + separator + '&'.join(kv_list)

    @classmethod
    def url_get_request(cls, url, kw):
        '''
            @desc get 请求url
        '''
        options = cls.create_request_options(**kw)
        response = False
        err = None
        try:
            if 'get_params' in kw:
                url = cls.attach_url_get_params(url, kw['get_params'])
            response = requests.get(url, **options)
        except Exception as e:
            err = str(e)
        finally:
            return response, err

    @classmethod
    def url_post_request(cls, url, data, kw):
        '''
            @desc get 请求url
        '''
        options = cls.create_request_options(**kw)
        response = False
        err = None
        try:
            if 'get_params' in kw:
                url = cls.attach_url_get_params(url, kw['get_params'])
            response = requests.post(url, **options)
        except requests.exceptions.RequestException as e:
            err = str(e)
        finally:
            return response, err
