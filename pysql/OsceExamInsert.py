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


from pyspark.sql.types import StructField, StringType, StructType
from pyspark import SparkContext
from pyspark.sql import HiveContext

from Utils import setLog, getConfig

reload(sys)
sys.setdefaultencoding('utf-8')


def md5(row):
    # 强制转码
    reload(sys)
    sys.setdefaultencoding('utf-8')
    temstr = "osce_exam.%s|osce_exam.%s|osce_exam.%s|osce_exam.%s|osce_exam.%s" \
             % (row.id, row.name, str(row.begintime), str(row.endtime), str(row.updatetime))
    m = hashlib.md5()
    m.update(temstr)
    return m.hexdigest()


if __name__ == '__main__':
    logger = setLog()
    # 定义客户标识
    cust_no = '1'
    isvalid = '1'
    slaveTempTable = 'osce_exam'
    etsTempTable = 'ets_osce_exam'
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
    ds_ets = sqlContext.sql(" select max(updatets) as max from %s " %(etsTempTable))
    pp = ds_ets.collect()[0]
    max_updates = pp.max
    slave_sql = ''
    try:
        if max_updates is not None:
            logger.info(u"ets库中的最大时间是：" + str(max_updates))
            slave_sql = " select id, name, begintime, endtime, updatetime " \
                        "  from  %s  where `updatetime` > '%s' " % (slaveTempTable, max_updates)
        else:
            logger.info(u"本次为初次抽取")
            slave_sql = " select id, name, begintime, endtime, updatetime " \
                        " from  %s  " %(slaveTempTable)
        ds_slave = sqlContext.sql(slave_sql)

        logger.info(u'slave 中 符合条件的记录数为：%s' % (ds_slave.count()))
        now_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logger.info(u'开始组装数据...')
        src_fields = json.dumps({'osce_exam': ['id', 'name', 'begintime', 'endtime', 'updatetime']})
        # 字段值
        filedvlue = ds_slave.map(lambda row: (row.id, row.name, str(row.begintime), str(row.endtime),
                                              cust_no, isvalid, src_fields,
                                              md5(row), now_time, str(row.updatetime)))
        # 创建列
        schemaString = "id,name,begintime,endtime,cust_no,isvalid,src_fields,src_fields_md5,createts,updatets"
        fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(",")]
        schema = StructType(fields)
        # 使用列名和字段值创建datafrom
        schemaObj = sqlContext.createDataFrame(filedvlue, schema)
        logger.info(u'组装数据完成...')
        # print schemaPeople
        # for row in schemaPeople:
        #     print row.id
        logger.info(u'开始执写入数据...')
        # 写入数据库
        schemaObj.write.insertInto(etsTempTable, overwrite=False)
        logger.info(u'写入完成')
    except Exception, e:
        # e.message 2.6 不支持
        logger.error(str(e))
        raise Exception(str(e))
    finally:
        sc.stop()

