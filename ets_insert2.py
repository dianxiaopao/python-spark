#!/usr/bin/env python
#coding=utf-8

'''
    Created on: 2017-09-06 by zb
    Purpose: spark accessing mysql
    
    Hive On Spark  解决方案 其目的是把Spark作为Hive的一个计算引擎，
    将Hive的查询作为Spark的任务提交到Spark集群上进行计算。通过该项目，
    可以提高Hive查询的性能，同时为已经部署了Hive或者Spark的用户提供了更加灵活的选择，
    从而进一步提高Hive和Spark的普及率。
'''

import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext

if __name__ == '__main__':
    sc = SparkContext(appName = "sql_insert")
    sqlContext = HiveContext(sc)
    #driver = "com.mysql.jdbc.Driver"
    dff = sqlContext.read.format("jdbc").options(url="jdbc:mysql://192.168.32.1:3306/testdjango?user=root&password=root&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull", dbtable="t_userinfo").load()
    dff.registerTempTable('t_userinfo')

    dft = sqlContext.read.format("jdbc").options(url="jdbc:mysql://192.168.32.1:3306/testdjango?user=root&password=root&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull", dbtable="t_userinfo2").load()
    dft.registerTempTable('t_userinfo2')
    ds = sqlContext.sql('select username,password,email from t_userinfo')
    print ds.collect()
    #ds 是datafram 类型 collect()转成 Row 可以遍历
    # for i in ds.collect():
    #     # print i.username
    #     # print i['username']
    #     sql = """insert into t_userinfo2 (`username`,`password`,`email`) VALUES ('%s','%s','%s')""" %(i.username,i.password,i.email)
    #     print  sql
    #     sqlContext.sql(sql)
    sqlContext.sql('insert into t_userinfo2 select username,password,email from t_userinfo')
    sqlContext.sql('select username, password from t_userinfo2').show(truncate=False)

    sc.stop()
