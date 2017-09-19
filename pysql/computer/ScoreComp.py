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

    ets_score = sqlContext.read.format("jdbc").options(url="jdbc:mysql://192.168.1.200:3307/bd_ets?user=root"
                                                     "&password=13851687968&useUnicode=true&characterEncoding=UTF-8"
                                                     "&zeroDateTimeBehavior=convertToNull", dbtable="ets_score",
                                                 driver="com.mysql.jdbc.Driver").load()
    ets_score.registerTempTable('ets_score')

    ets_learn = sqlContext.read.format("jdbc").options(url="jdbc:mysql://192.168.1.200:3307/bd_ets?user=root"
                                                      "&password=13851687968&useUnicode=true&characterEncoding=UTF-8"
                                                      "&zeroDateTimeBehavior=convertToNull", dbtable="ets_learn",
                                                  driver="com.mysql.jdbc.Driver").load()
    ets_learn.registerTempTable('ets_learn')

    ets_osce_score = sqlContext.read.format("jdbc").options(url="jdbc:mysql://192.168.1.200:3307/bd_ets?user=root"
                                                      "&password=13851687968&useUnicode=true&characterEncoding=UTF-8"
                                                      "&zeroDateTimeBehavior=convertToNull", dbtable="ets_osce_score",
                                                  driver="com.mysql.jdbc.Driver").load()
    ets_osce_score.registerTempTable('ets_osce_score')

    ets_score_ds = sqlContext.sql("select s.totalscore as totalscore from ets_score s, ets_learn l "
                        "where s.scoresheetcode = l.scoresheetcode and l.type = '%s'" % type)
    avgscore = sqlContext.sql("select avg(s.totalscore) as avg from ets_score s, ets_learn l "
                         "where s.scoresheetcode = l.scoresheetcode and l.type = '%s'" % type).collect()[0].avg
    avgscore = (0 if avgscore == None else avgscore)

    # 使用spark提供的计算方法
    maxscore = ets_score_ds.agg(F.max(ets_score_ds['totalscore'])).collect()[0]['max(totalscore)']
    maxscore = (0 if maxscore == None else maxscore)

    minscore = ets_score_ds.agg(F.min(ets_score_ds['totalscore'])).collect()[0]['min(totalscore)']
    minscore = (0 if minscore == None else minscore)

    # osce 相关

    ets_osce_score_ds = sqlContext.sql("select * from ets_osce_score")
    print '_' * 150
    print maxscore
    print minscore
    print avgscore
    print '_' * 150
    temds = ets_osce_score_ds.groupBy(['examid', 'examineeid']).agg(F.sum(ets_osce_score_ds['totalscore']))
    print  temds.sort(temds['sum(totalscore)'].desc()).collect()
    # .sort(ets_osce_score_ds['sum(totalscore)'].desc()).collect()
    #print reportjson

    # if reportjson is not None:
    #     reportjsonRDD = sc.parallelize(json.dumps(reportjson))
    #     print reportjsonRDD
    #     reportds = sqlContext.read.json(reportjsonRDD)
    #     print reportds.take(2)
    #     print reportds.count()
    #     # logging.info(ets_osce_das_admin_report_ds.collect()[0]['report'])
    #     # print ets_osce_das_admin_report_ds.collect()[0]['report']

    sc.stop()


if __name__ == '__main__':
    setLog()
    computer(2)


