#!/usr/bin/env python
# -*- coding: utf-8 -*-

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


def md5(row):
    # 强制转码
    reload(sys)
    sys.setdefaultencoding('utf-8')
    m = hashlib.md5()
    temstr = "Schedule.%s|Schedule.%s|Schedule.%s|Schedule.%s|Schedule.%s|Schedule.%s|Schedule.%s" \
             "|Schedule.%s|Schedule.%s" % (row.id, row.course_id, row.start_dt, row.start_time, row.end_time,
                                           row.std_total, row.place_id, row.status, row.updated_at)
    m.update(temstr)
    return m.hexdigest()

if __name__ == '__main__':
    setLog()
    # 定义客户标识
    cust_no = '1'
    isvalid = '1'
    slaveTempTable = 'schedule_slave'
    etsTempTable = 'ets_schedule'
    sc = SparkContext(appName="ScheduleStdInsertInsert")
    sqlContext = HiveContext(sc)
    dff = sqlContext.read.format("jdbc").options(url="jdbc:mysql://192.168.1.200:3306/osce1030?user=root"
                                                     "&password=misrobot_whu&useUnicode=true&characterEncoding=UTF-8"
                                                     "&zeroDateTimeBehavior=convertToNull", dbtable="Schedule",
                                                     driver="com.mysql.jdbc.Driver").load()
    dff.registerTempTable(slaveTempTable)

    dft = sqlContext.read.format("jdbc").options(url="jdbc:mysql://192.168.1.200:3307/bd_ets?user=root"
                                                     "&password=13851687968&useUnicode=true&characterEncoding=UTF-8"
                                                     "&zeroDateTimeBehavior=convertToNull", dbtable="ets_schedule",
                                                     driver="com.mysql.jdbc.Driver").load()
    dft.registerTempTable(etsTempTable)
    try:
        ds_ets = sqlContext.sql(" select max(updatets) as max from %s " % (etsTempTable))
        pp = ds_ets.collect()[0]
        max_updates = pp.max
        slave_sql = ''
        if max_updates is not None:
            logging.info(u"ets库中的最大时间是：" + str(max_updates))
            slave_sql = " select id, course_id, start_dt, start_time, end_time, std_total, place_id, status, updated_at " \
                        "  from  %s  where `updated_at` > '%s' " % (slaveTempTable, max_updates)
        else:
            logging.info(u"本次为初次抽取")
            slave_sql = " select id, course_id, start_dt, start_time, end_time, std_total, place_id, status, updated_at " \
                        " from  %s  " % (slaveTempTable)
        ds_slave = sqlContext.sql(slave_sql)
        logging.info(u'slave 中 符合条件的记录数为：%s' % (ds_slave.count()))
        now_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logging.info(u'开始组装数据...')
        src_fields = json.dumps({'Schedule': ['id', 'course_id', 'start_dt', 'start_time', 'end_time', 'std_total', 'place_id', 'status', 'updated_at']})
        # 字段值
        filedvlue = ds_slave.map(lambda row: (row.id, row.course_id, row.start_dt, row.start_time, row.end_time, row.std_total, row.place_id, row.status, cust_no, isvalid, src_fields,
                                 md5(row), now_time, row.updated_at))
        # 创建列
        schemaString = "id,course_id,start_dt,start_time,end_time," \
                       "std_total,place_id,status,cust_no,isvalid,src_fields,src_fields_md5,createts,updatets"
        fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(",")]
        schema = StructType(fields)
        # 使用列名和字段值创建datafrom
        schemaObject = sqlContext.createDataFrame(filedvlue, schema)
        logging.info(u'组装数据完成')
        # print schemaPeople
        # for row in schemaPeople:
        #     print row.id
        logging.info(u'开始执写入数据...')
        # 写入数据库
        schemaObject.write.insertInto(etsTempTable, overwrite=False)
        logging.info(u'写入完成')
    except Exception, e:
        # e.message 2.6 不支持
        logging.error(str(e))
        raise Exception(str(e))
    finally:
        sc.stop()
