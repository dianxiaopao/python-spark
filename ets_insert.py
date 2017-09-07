#!/usr/bin/env python
#coding=utf-8

'''
    Created on: 2017-09-06 by AndyZhou
    Purpose: spark accessing mysql
'''

import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext

if __name__ == '__main__':
    sc = SparkContext(appName = "sql_insert")
    sqlContext = HiveContext(sc)

    dff = sqlContext.read.format("jdbc").options(url="jdbc:mysql://192.168.1.200:3306/osce1030?user=root&password=misrobot_whu&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull", dbtable="pf_timer").load()
    dff.registerTempTable('pf_timer_f')

    dft = sqlContext.read.format("jdbc").options(url="jdbc:mysql://192.168.1.200:3307/andytest?user=root&password=13851687968&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull", dbtable="pf_timer").load()
    dft.registerTempTable('pf_timer_t')

    sqlContext.sql('insert into pf_timer_t select * from pf_timer_f')
    sqlContext.sql('select timerid, caption from pf_timer_t').show(truncate=False)

    sc.stop()
