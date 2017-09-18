#!/usr/bin/env python
# coding=utf-8

"""
   author:zb
   create_at:2017-9-8 09:37:45
"""
import hashlib
import os
import sys
import datetime
import json
import logging

from os import path

from pyspark.sql.types import StructField, StringType, StructType

reload(sys)
sys.setdefaultencoding('utf-8')


from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import functions as F


'''
    日志模块
'''


def setLog():
    logger = logging.getLogger()
    # spark中 DEBUG 级别无法使用
    logger.setLevel(logging.INFO)  # Log等级总开关

    # 第二步，创建一个handler，用于写入日志文件
    logfile = os.path.join(path.dirname(__file__), 'logger.txt')
    fh = logging.FileHandler(logfile, mode='a')
    fh.setLevel(logging.INFO)  # 输出到file的log等级的开关

    # 第三步，再创建一个handler，用于输出到控制台
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)  # 输出到console的log等级的开关

    # 第四步，定义handler的输出格式
    formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # 第五步，将logger添加到handler里面
    logger.addHandler(fh)
    logger.addHandler(ch)


def computer(type):
    appname = 'score' + '_computer'
    sc = SparkContext(appName=appname)
    sqlContext = HiveContext(sc)
    # driver = "com.mysql.jdbc.Driver"
    dff = sqlContext.read.format("jdbc").options(url="jdbc:mysql://192.168.1.200:3306/osce1030?user=root"
                                                     "&password=misrobot_whu&useUnicode=true&characterEncoding=UTF-8"
                                                     "&zeroDateTimeBehavior=convertToNull", dbtable="Score",
                                                 driver="com.mysql.jdbc.Driver").load()
    dff.registerTempTable('score_slave')

    dft = sqlContext.read.format("jdbc").options(url="jdbc:mysql://192.168.1.200:3307/bd_ets?user=root"
                                                     "&password=13851687968&useUnicode=true&characterEncoding=UTF-8"
                                                     "&zeroDateTimeBehavior=convertToNull", dbtable="ets_score",
                                                 driver="com.mysql.jdbc.Driver").load()
    dft.registerTempTable('ets_score')

    dft2 = sqlContext.read.format("jdbc").options(url="jdbc:mysql://192.168.1.200:3307/bd_ets?user=root"
                                                      "&password=13851687968&useUnicode=true&characterEncoding=UTF-8"
                                                      "&zeroDateTimeBehavior=convertToNull", dbtable="ets_learn",
                                                  driver="com.mysql.jdbc.Driver").load()
    dft2.registerTempTable('ets_learn')

    ds = sqlContext.sql("select s.totalscore as totalscore from ets_score s, ets_learn l "
                        "where s.scoresheetcode = l.scoresheetcode and l.type = '%s'" % type)
    avg = sqlContext.sql("select avg(s.totalscore) as avg from ets_score s, ets_learn l "
                         "where s.scoresheetcode = l.scoresheetcode and l.type = '%s'" % type).collect()[0].avg
    avgval = (0 if avg == None else avg)

    # 使用spark提供的计算方法
    maxvalold = ds.agg(F.max(ds['totalscore'])).collect()[0]['max(totalscore)']
    maxval = (0 if maxvalold == None else maxvalold)

    minold = ds.agg(F.min(ds['totalscore'])).collect()[0]['min(totalscore)']
    minval = (0 if minold == None else minold)

    print '_' * 50
    print maxval
    print minval
    print avgval
    print '++++++++++++++++++++++++++++++++++'
    sc.stop()


if __name__ == '__main__':
    setLog()
    computer(2)


