#!/usr/bin/env python
#coding=utf-8
import hashlib

import json

import logging
import os
import time

from os import path

import datetime

from Utils import setLog

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