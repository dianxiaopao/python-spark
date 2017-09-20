#!/usr/bin/env python
#coding=utf-8
import hashlib

import json

import logging
import os
import time

from os import path

import datetime

'''
    日志模块
'''


def setLog():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # Log等级总开关

    # 第二步，创建一个handler，用于写入日志文件
    logfile = os.path.join(path.dirname(__file__), 'logger.txt')
    fh = logging.FileHandler(logfile, mode='w')
    fh.setLevel(logging.DEBUG)  # 输出到file的log等级的开关

    # 第三步，再创建一个handler，用于输出到控制台
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)  # 输出到console的log等级的开关

    # 第四步，定义handler的输出格式
    formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # 第五步，将logger添加到handler里面
    logger.addHandler(fh)
    logger.addHandler(ch)


now_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print now_time

m = hashlib.md5()
m.update('password')
psw = m.hexdigest()
print psw

src_fields = {'Score':['id','starttime','endtime','operatorstudentid','graderstudentid','scoresheetcode','totalscore']}
src_fields = json.dumps(src_fields)
print src_fields
setLog()
logging.debug(33)

# 时间戳
timestamp = 1476667800
#转换成localtime
time_local = time.localtime(timestamp)
#转换成新的时间格式(2016-05-05 20:28:54)
dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
print dt