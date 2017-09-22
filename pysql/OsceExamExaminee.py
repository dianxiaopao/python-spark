#!/usr/bin/env python
# coding=utf-8

"""
   @author:zb
   @create_at:2017-9-14 09:37:45
"""
import hashlib
import sys
import datetime
import json
import logging


from pyspark.sql.types import StructField, StringType, StructType

from Utils import execute_sql_ets, setLog, getConfig
from pyspark import SparkContext
from pyspark.sql import HiveContext

reload(sys)
sys.setdefaultencoding('utf-8')


def md5(row):
    # 强制转码
    reload(sys)
    sys.setdefaultencoding('utf-8')
    temstr = "GradeItem.%s|GradeItem.%s|GradeItem.%s" \
             % (row.id, row.examineeid, row.examid)
    m = hashlib.md5()
    m.update(temstr)
    return m.hexdigest()

if __name__ == '__main__':
    setLog()
    # 定义客户标识
    cust_no = '1'
    isvalid = '1'
    slaveTempTable = 'osce_exam_examinee'
    etsTempTable = 'ets_osce_exam_examinee'
    ets_url = getConfig().get('db', 'ets_url_all')
    slave_url = getConfig().get('db', 'slave_url')
    driver = "com.mysql.jdbc.Driver"
    appname = etsTempTable + '_insert'
    sc = SparkContext(appName=appname)
    sqlContext = HiveContext(sc)
    dff = sqlContext.read.format("jdbc").options(url=slave_url, dbtable=slaveTempTable, driver=driver).load()
    dff.registerTempTable(slaveTempTable)
    dft = sqlContext.read.format("jdbc").options(url=ets_url, dbtable=etsTempTable, driver=driver).load()
    dft.registerTempTable(etsTempTable)
    try:
        slave_sql = " select id, examineeid, examid " \
                    " from  %s  " % (slaveTempTable)
        ds_slave = sqlContext.sql(slave_sql)
        logging.info(u"覆盖式插入:共%s条数据" % ds_slave.count())
        # sqlContext.sql(" delete from %s " % etsTempTable)
        ddlsql = " truncate table %s " % etsTempTable
        # 删除表中数据 使用 jdbc方式
        execute_sql_ets(ddlsql)
        now_time = datetime.datetime.now()
        logging.info(u'开始组装数据...')
        src_fields = json.dumps({'osce_exam_examinee': ['id', 'examineeid', 'examid']})
        # 字段值
        filedvlue = ds_slave.map(lambda row: (row.id, row.examineeid, row.examid, cust_no, isvalid, src_fields,
                                 md5(row), now_time, now_time))
        # 创建列
        schemaString = "id,examineeid,examid,cust_no,isvalid,src_fields,src_fields_md5,createts,updatets"
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
