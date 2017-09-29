﻿#!/usr/bin/env python
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

from Utils import setLog, getConfig, jsonTranfer, loadjson

reload(sys)
sys.setdefaultencoding('utf-8')


def md5(row):
    # 强制转码
    reload(sys)
    sys.setdefaultencoding('utf-8')
    temstr = "QuestionVoteRecord.%s|QuestionVoteRecord.%s|QuestionVoteRecord.%s|QuestionVoteRecord.%s" \
             % (row.id, row.userid, row.vote_id, str(row.created_at))
    m = hashlib.md5()
    m.update(temstr)
    return m.hexdigest()


def do_ets_task(sc, ets_dburl_env, wfc):
    # 定义客户标识
    cust_no = '1'
    isvalid = '1'
    slaveTempTable = 'QuestionVoteRecord'
    etsTempTable = wfc
    ets_dburl_env_dict = loadjson(ets_dburl_env)
    ets_url = ets_dburl_env_dict.get('ets_questionvoterecord', '').get('dst', '')
    slave_url = ets_dburl_env_dict.get('ets_questionvoterecord', '').get('src', '')
    driver = "com.mysql.jdbc.Driver"
    sqlContext = HiveContext(sc)
    # driver = "com.mysql.jdbc.Driver"
    dff = sqlContext.read.format("jdbc").options(url=slave_url, dbtable=slaveTempTable, driver=driver).load()
    dff.registerTempTable(slaveTempTable)
    dft = sqlContext.read.format("jdbc").options(url=ets_url, dbtable=etsTempTable, driver=driver).load()
    dft.registerTempTable(etsTempTable)
    ds_ets = sqlContext.sql(" select max(updatets) as max from %s " % etsTempTable)
    pp = ds_ets.collect()[0]
    max_updates = pp.max
    slave_sql = ''
    try:
        if max_updates is not None:
            print(u"ets库中的最大时间是：" + str(max_updates))
            slave_sql = " select id, userid, vote_id, created_at " \
                        "  from  %s  where `created_at` > '%s' " % (slaveTempTable, max_updates)
        else:
            print(u"本次为初次抽取")
            slave_sql = " select id, userid, vote_id, created_at " \
                        " from %s  " % slaveTempTable
        ds_slave = sqlContext.sql(slave_sql)
        print(u'slave 中 符合条件的记录数为：%s' % (ds_slave.count()))
        now_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(u'开始组装数据...')
        src_fields = json.dumps({'QuestionVoteRecord': ['id', 'userid', 'vote_id', 'created_at']})
        # 字段值
        filedvlue = ds_slave.map(lambda row: (row.id, row.userid, row.vote_id, cust_no,
                                              isvalid,
                                              md5(row), now_time, str(row.created_at)))
        # 创建列
        schemaString = "id,userid,vote_id,cust_no,isvalid,src_fields_md5,createts,updatets"
        fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(",")]
        schema = StructType(fields)
        # 使用列名和字段值创建datafrom
        schemaObj = sqlContext.createDataFrame(filedvlue, schema)
        print(u'组装数据完成...')
        # print schemaPeople
        # for row in schemaPeople:
        #     print row.id
        print(u'开始执写入数据...')
        # 写入数据库
        schemaObj.write.insertInto(etsTempTable)
        print(u'写入完成')
    except Exception, e:
        # e.message 2.6 不支持
        print (str(e))
        raise Exception(str(e))


if __name__ == '__main__':
    appname = 'rr_insert'
    sc = SparkContext(appName=appname)
    cp = getConfig()
    ets_dburl_env = {"ets_questionvoterecord": {
        "src": cp.get('db', 'slave_url'),
        "dst": cp.get('db', 'ets_url_all')}}
    wfc = "ets_questionvoterecord"
    do_ets_task(sc, jsonTranfer(ets_dburl_env), wfc)

