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
import traceback

from pyspark import SparkContext
from pyspark.sql.types import StructField, StringType, StructType

from pyspark.sql import HiveContext

from Utils import execute_sql_ets, loadjson, jsonTranfer, getdbinfo, getConfig

reload(sys)
sys.setdefaultencoding('utf-8')


def md5(row):
    # 强制转码
    reload(sys)
    sys.setdefaultencoding('utf-8')
    temstr = "GradeItem.%s|GradeItem.%s|GradeItem.%s|GradeItem.%s" \
             % (row.id, row.learn_id, row.learn_type, row.scoresheetcode)
    m = hashlib.md5()
    m.update(temstr)
    return m.hexdigest()


def do_ets_task(sc, ets_dburl_env, wfc):
    # 定义客户标识
    cust_no = '1'
    isvalid = '1'
    slaveTempTable = 'GradeItem'
    etsTempTable = wfc
    ets_dburl_env_dict = loadjson(ets_dburl_env)
    ets_url = ets_dburl_env_dict.get('ets_gradeitem', '').get('dst', '')
    slave_url = ets_dburl_env_dict.get('ets_gradeitem', '').get('src', '')
    driver = "com.mysql.jdbc.Driver"
    sqlContext = HiveContext(sc)
    dff = sqlContext.read.format("jdbc").options(url=slave_url, dbtable=slaveTempTable, driver=driver).load()
    dff.registerTempTable(slaveTempTable)

    dft = sqlContext.read.format("jdbc").options(url=ets_url, dbtable=etsTempTable, driver=driver).load()
    dft.registerTempTable(etsTempTable)
    try:
        slave_sql = " select id, learn_id, learn_type, scoresheetcode " \
                    " from  %s  " % (slaveTempTable)
        ds_slave = sqlContext.sql(slave_sql)
        print(u"覆盖式插入:共%s条数据" % ds_slave.count())
        # sqlContext.sql(" delete from %s " % etsTempTable)
        ddlsql = " truncate table %s " % etsTempTable
        # 删除表中数据 使用 jdbc方式
        dbinfo = getdbinfo(ets_url)
        execute_sql_ets(ddlsql, dbinfo)
        now_time = datetime.datetime.now()
        print(u'开始组装数据...')
        src_fields = json.dumps({'GradeItem': ['id', 'learn_id', 'learn_type', 'scoresheetcode']})
        # 字段值
        filedvlue = ds_slave.map(
            lambda row: (row.id, row.learn_id, row.learn_type, row.scoresheetcode, cust_no, isvalid,
                         md5(row), now_time, now_time))
        # 创建列
        schemaString = "id,learn_id,learn_type,scoresheetcode,cust_no,isvalid,src_fields_md5,createts,updatets"
        fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(",")]
        schema = StructType(fields)
        # 使用列名和字段值创建datafrom
        schemaPeople = sqlContext.createDataFrame(filedvlue, schema)
        print(u'组装数据完成')
        # print schemaPeople
        # for row in schemaPeople:
        #     print row.id
        print(u'开始执写入数据...')
        # 写入数据库
        schemaPeople.write.insertInto(etsTempTable)
        print(u'写入完成')
    except Exception, e:
        # e.message 2.6 不支持
        print(traceback.print_exc())
        print(str(e))
        raise Exception(str(e))


if __name__ == '__main__':
    appname = 'rr_insert'
    sc = SparkContext(appName=appname)
    cp = getConfig()
    ets_dburl_env = {"ets_gradeitem": {
        "src": cp.get('db', 'slave_url'),
        "dst": cp.get('db', 'ets_url_all')}}
    wfc = "ets_gradeitem"
    do_ets_task(sc, jsonTranfer(ets_dburl_env), wfc)
