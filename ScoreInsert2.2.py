#!/usr/bin/env python
# coding=utf-8

"""
   author:zb
   create_at:2017-9-8 09:37:45
"""
import hashlib
import sys
import datetime
import json

import logging
from pyspark import SparkContext
from pyspark.sql import HiveContext
from Utils import setLog

reload(sys)
sys.setdefaultencoding('utf-8')

if __name__ == '__main__':
    logger = setLog()
    # 定义客户标识
    cust_no = '1'
    sc = SparkContext(appName="ScoreInsert")
    sqlContext = HiveContext(sc)
    # driver = "com.mysql.jdbc.Driver"
    dff = sqlContext.read.format("jdbc").options(url="jdbc:mysql://192.168.1.200:3306/osce1030?user=root"
                                                     "&password=misrobot_whu&useUnicode=true&characterEncoding=UTF-8"
                                                     "&zeroDateTimeBehavior=convertToNull", dbtable="Score",
                                                     driver="com.mysql.jdbc.Driver").load()
    dff.registerTempTable('Score_slave')

    dft = sqlContext.read.format("jdbc").options(url="jdbc:mysql://192.168.1.200:3307/bd_ets?user=root"
                                                     "&password=13851687968&useUnicode=true&characterEncoding=UTF-8"
                                                     "&zeroDateTimeBehavior=convertToNull", dbtable="ets_score",
                                                      driver="com.mysql.jdbc.Driver").load()
    dft.registerTempTable('ets_score')
    ds_ets = sqlContext.sql(" select max(updatets) as max from ets_score ")
    pp = ds_ets.collect()[0]
    max_updates = pp.max
    slave_sql = ''
    try:
        if max_updates is not  None:
            logger.info(u"ets库中的最大时间是：" + str(max_updates))
            slave_sql = "select id, starttime, endtime, updatetime, operatorstudentid, graderstudentid, scoresheetcode, totalscore" \
                        " from Score_slave where `updatetime` >= '%s' " % (max_updates)
        else:
            logger.info(u"本次为初次抽取")
            slave_sql = " select id, starttime, endtime, updatetime, operatorstudentid, graderstudentid, scoresheetcode, totalscore" \
                        " from Score_slave  "
        ds_slave = sqlContext.sql(slave_sql)
        logger.info(u'slave 中 符合条件的记录数为：%s' %(ds_slave.count()))
        m = hashlib.md5()
        now_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logger.info(u'开始执行抽取数据...')
        for row in ds_slave.collect():
            src_fields = {'Score': ['id', 'starttime', 'endtime', 'updatetime', 'operatorstudentid', 'graderstudentid', 'scoresheetcode', 'totalscore']}
            src_fields = json.dumps(src_fields)
            src_fieldsvul = "score.%s|Score.%s|Score.%s|Score.%s|Score.%s|Score.%s|Score.%s|Score.%s" \
                            % (row.id, row.starttime, row.endtime, row.updatetime, row.operatorstudentid,
                               row.graderstudentid,
                               row.scoresheetcode, row.totalscore)
            print src_fields
            print src_fieldsvul
            m.update(src_fieldsvul)
            src_fields_md5 = m.hexdigest()
            sql = "insert into  ets_score VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s' )"\
                  % (row.id, row.starttime, row.endtime, row.operatorstudentid, row.graderstudentid,
                     row.scoresheetcode, row.totalscore,
                     cust_no, '1', src_fields, src_fields_md5, now_time, row.updatetime)
            sqlContext.sql(sql)
        logger.info(u'抽取完成')
    except Exception, e:
        logger.error(e.message)
        raise Exception(e.message)
    finally:
        sc.stop()
