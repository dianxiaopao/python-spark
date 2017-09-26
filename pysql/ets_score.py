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


from pyspark.sql.types import StructField, StructType, StringType
from pyspark import SparkContext
from pyspark.sql import HiveContext

from Utils import setLog, getConfig, jsonTranfer, loadjson

reload(sys)
sys.setdefaultencoding('utf-8')


def md5(row):
    # 强制转码
    reload(sys)
    sys.setdefaultencoding('utf-8')
    temstr = "score.%s|Score.%s|Score.%s|Score.%s|Score.%s|Score.%s|Score.%s|Score.%s" \
             % (row.id, str(row.starttime), str(row.endtime), row.operatorstudentid,
                row.graderstudentid, row.scoresheetcode, row.totalscore, str(row.updatetime))
    m = hashlib.md5()
    m.update(temstr)
    return m.hexdigest()


def do_ets_task(sc, ets_dburl_env, wfc):
    # 定义客户标识
    cust_no = '1'
    isvalid = '1'
    slaveTempTable = 'Score'
    etsTempTable = wfc
    ets_dburl_env_dict = loadjson(ets_dburl_env)
    ets_url = ets_dburl_env_dict.get('ets_score', '').get('dst', '')
    slave_url = ets_dburl_env_dict.get('ets_score', '').get('src', '')
    driver = "com.mysql.jdbc.Driver"
    sqlContext = HiveContext(sc)
    # driver = "com.mysql.jdbc.Driver"
    dff = sqlContext.read.format("jdbc").options(url=slave_url, dbtable=slaveTempTable, driver=driver).load()
    dff.registerTempTable(slaveTempTable)

    dft = sqlContext.read.format("jdbc").options(url=ets_url, dbtable=etsTempTable, driver=driver).load()
    dft.registerTempTable(etsTempTable)
    ds_ets = sqlContext.sql(" select max(updatets) as max from %s " % (etsTempTable))
    pp = ds_ets.collect()[0]
    max_updates = pp.max
    slave_sql = ''
    try:
        if max_updates is not  None:
            print(u"ets库中的最大时间是：" + str(max_updates))
            slave_sql = "select id, starttime, endtime, updatetime, operatorstudentid, graderstudentid, scoresheetcode, totalscore" \
                        " from %s where `updatetime` > '%s' " % (slaveTempTable, max_updates)
        else:
            print(u"本次为初次抽取")
            slave_sql = " select id, starttime, endtime, updatetime, operatorstudentid, graderstudentid, scoresheetcode, totalscore" \
                        " from  %s  " % (slaveTempTable)
        ds_slave = sqlContext.sql(slave_sql)
        print(u'slave 中 符合条件的记录数为：%s' %(ds_slave.count()))
        now_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(u'开始组装数据...')
        src_fields = json.dumps({'Score': ['id', 'starttime', 'endtime', 'operatorstudentid', 'graderstudentid', 'scoresheetcode', 'totalscore', 'updatetime']})
        # 字段值
        filedvlue = ds_slave.map(lambda row: (row.id, str(row.starttime), str(row.endtime), row.operatorstudentid,
                                              row.graderstudentid, row.scoresheetcode, row.totalscore,  cust_no,
                                              isvalid, md5(row), now_time, str(row.updatetime)))
        # 创建列
        schemaString = "id,starttime,endtime,operatorstudentid,graderstudentid,scoresheetcode,totalscore,cust_no,isvalid,src_fields_md5,createts,updatets"
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
        schemaObj.write.insertInto(etsTempTable, overwrite=False)
        print(u'写入完成')
    except Exception, e:
        # e.message 2.6 不支持
        print (str(e))
        raise Exception(str(e))


if __name__ == '__main__':
    appname = 'rr_insert'
    sc = SparkContext(appName=appname)
    ets_dburl_env = {"ets_score": {
        "src": "jdbc:mysql://192.168.1.200:3306/osce1030?user=root&password=misrobot_whu&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull",
        "dst": "jdbc:mysql://192.168.1.200:3307/bd_ets?user=root&password=13851687968&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull"}}
    wfc = "ets_score"
    do_ets_task(sc, jsonTranfer(ets_dburl_env), wfc)

