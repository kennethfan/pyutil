#!/usr/bin/python
# -*- coding: utf-8 -*-
# Desc: 发送邮件工具
# Date: 2016-04-01


import os
import smtplib
from email.header import Header
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase


class MailUtil:
    @staticmethod
    def mail(self, config, subject, content, filename=None):
        '''
            @desc 发送邮件
            @param  dic config 邮件配置
            @param string subject 邮件主题
            @param string content 邮件内容
            @param string filename 附件名称
            @return bool, string
        '''
        MAIL_LIST = config['mail_list']
        MAIL_HOST = config['mail_host']
        MAIL_USER = config['mail_user']
        MAIL_PASS = config['mail_pass']
        #MAIL_POSTFIX = config['mail_postfix']
        MAIL_FROM = config['mail_from']

        try:
            message = MIMEMultipart()
            message.attach(MIMEText(content, 'html', _charset='UTF-8'))
            message['Subject'] = Header(subject, 'utf-8')
            message['From'] = MAIL_FROM
            message['To'] = ';'.join(MAIL_LIST)
            if filename is not None and os.path.exists(filename):
                attachment = MIMEBase('application', 'octec-stream')
                attachment.set_payload(open(filename, 'rb').read(), 'utf-8')
                attachment.add_header('Content-Disposition', 'attachment',
                                      filename=os.path.basename(filename))
                message.attach(attachment)
            smtp = smtplib.SMTP()
            smtp.connect(MAIL_HOST)
            smtp.login(MAIL_USER, MAIL_PASS)
            smtp.sendmail(MAIL_FROM, MAIL_LIST, message.as_string())
            smtp.quit()
            return True, ''
        except Exception, errmsg:
            return False, errmsg
