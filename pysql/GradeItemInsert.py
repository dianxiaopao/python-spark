﻿#!/usr/bin/env python
# coding=utf-8

"""
   @author:zb
   @create_at:2017-9-14 09:37:45
"""
import hashlib
import os
import sys
import datetime
import json
import logging

from os import path

from pyspark.sql.types import StructField, StringType, StructType

from Utils import execute_sql_ets, setLog

reload(sys)
sys.setdefaultencoding('utf-8')

from pyspark import SparkContext
from pyspark.sql import HiveContext


def md5(row):
    # 强制转码
    reload(sys)
    sys.setdefaultencoding('utf-8')
    temstr = "GradeItem.%s|GradeItem.%s|GradeItem.%s|GradeItem.%s" \
             % (row.id, row.learn_id, row.learn_type, row.scoresheetcode)
    m = hashlib.md5()
    m.update(temstr)
    return m.hexdigest()

if __name__ == '__main__':
    setLog()
    # 定义客户标识
    cust_no = '1'
    isvalid = '1'
    slaveTempTable = 'gradeitem_slave'
    etsTempTable = 'ets_gradeitem'
    appname = etsTempTable + '_insert'
    sc = SparkContext(appName=appname)
    sqlContext = HiveContext(sc)
    dff = sqlContext.read.format("jdbc").options(url="jdbc:mysql://192.168.1.200:3306/osce1030?user=root"
                                                     "&password=misrobot_whu&useUnicode=true&characterEncoding=UTF-8"
                                                     "&zeroDateTimeBehavior=convertToNull", dbtable="GradeItem",
                                                     driver="com.mysql.jdbc.Driver").load()
    dff.registerTempTable(slaveTempTable)

    dft = sqlContext.read.format("jdbc").options(url="jdbc:mysql://192.168.1.200:3307/bd_ets?user=root"
                                                     "&password=13851687968&useUnicode=true&characterEncoding=UTF-8"
                                                     "&zeroDateTimeBehavior=convertToNull", dbtable="ets_gradeitem",
                                                     driver="com.mysql.jdbc.Driver").load()
    dft.registerTempTable(etsTempTable)
    try:
        slave_sql = " select id, learn_id, learn_type, scoresheetcode " \
                    " from  %s  " % (slaveTempTable)
        ds_slave = sqlContext.sql(slave_sql)
        logging.info(u"覆盖式插入:共%s条数据" % ds_slave.count())
        # sqlContext.sql(" delete from %s " % etsTempTable)
        ddlsql = " truncate table %s " % etsTempTable
        # 删除表中数据 使用 jdbc方式
        execute_sql_ets(ddlsql)
        now_time = datetime.datetime.now()
        logging.info(u'开始组装数据...')
        src_fields = json.dumps({'GradeItem': ['id', 'learn_id', 'learn_type', 'scoresheetcode']})
        # 字段值
        filedvlue = ds_slave.map(lambda row: (row.id, row.learn_id, row.learn_type, row.scoresheetcode, cust_no, isvalid, src_fields,
                                 md5(row), now_time, now_time))
        # 创建列
        schemaString = "id,learn_id,learn_type,scoresheetcode,cust_no,isvalid,src_fields,src_fields_md5,createts,updatets"
        fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(",")]
        schema = StructType(fields)
        # 使用列名和字段值创建datafrom
        schemaPeople = sqlContext.createDataFrame(filedvlue, schema)
        logging.info(u'组装数据完成')
        # print schemaPeople
        # for row in schemaPeople:
        #     print row.id
        logging.info(u'开始执写入数据...')
        # 写入数据库
        schemaPeople.write.insertInto(etsTempTable)
        logging.info(u'写入完成')
    except Exception, e:
        # e.message 2.6 不支持
        logging.error(str(e))
        raise Exception(str(e))
    finally:
        sc.stop()
