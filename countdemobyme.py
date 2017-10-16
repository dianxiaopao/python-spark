#!/usr/bin/env python
#coding=utf-8

"""
    Created on: 2017-09-06 by zb
    Purpose: spark accessing mysql
    
    Hive On Spark  解决方案 其目的是把Spark作为Hive的一个计算引擎，
    将Hive的查询作为Spark的任务提交到Spark集群上进行计算。通过该项目，
    可以提高Hive查询的性能，同时为已经部署了Hive或者Spark的用户提供了更加灵活的选择，
    从而进一步提高Hive和Spark的普及率。
    
    t_userinfo3 三个字段 id  
                        gender 性别 F-女 M-男   
                        height 身高
"""

import sys
reload(sys)
sys.setdefaultencoding('utf-8')


from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import functions as F

if __name__ == '__main__':
    sc = SparkContext(appName="sql_insert")
    sqlContext = HiveContext(sc)
    # driver = "com.mysql.jdbc.Driver"
    dff = sqlContext.read.format("jdbc").options(url="jdbc:mysql://192.168.32.1:3306/testdjango?user=root&password=root"
                                                     "&useUnicode=true&characterEncoding=UTF-8"
                                                     "&zeroDateTimeBehavior=convertToNull", dbtable="t_userinfo3",driver = "com.mysql.jdbc.Driver").load()
    dff.registerTempTable('t_userinfo3')
    ds = sqlContext.sql('select id,gender,height from t_userinfo3')
    # 计算   男人中身高大于 175cm的人
    dss = ds.filter(ds['gender'] == 'M').filter(ds['height'] > 175).sort(ds['height'].desc())
    # 展示100 条数据
    dss.show(100)
    # row里面取100条
    print(dss.take(50))
    print (dss.count())
    # 统计男性 平均身高
    print (ds.filter(ds['gender'] == 'M').agg(F.avg(ds['height'])).collect())
    sc.stop()
