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
    setLog()
    # 定义客户标识
    cust_no = '1'
    isvalid = '1'
    slaveTempTable = 'schedule_std_slave'
    etsTempTable = 'ets_schedule_std'
    sc = SparkContext(appName="ScheduleStdInsertInsert")
    sqlContext = HiveContext(sc)
    dff = sqlContext.read.format("jdbc").options(url="jdbc:mysql://192.168.1.200:3306/osce1030?user=root"
                                                     "&password=misrobot_whu&useUnicode=true&characterEncoding=UTF-8"
                                                     "&zeroDateTimeBehavior=convertToNull", dbtable="Schedule_Std",
                                                 driver="com.mysql.jdbc.Driver").load()
    dff.registerTempTable(slaveTempTable)

    dft = sqlContext.read.format("jdbc").options(url="jdbc:mysql://192.168.1.200:3307/bd_ets?user=root"
                                                     "&password=13851687968&useUnicode=true&characterEncoding=UTF-8"
                                                     "&zeroDateTimeBehavior=convertToNull", dbtable="ets_schedule_std",
                                                 driver="com.mysql.jdbc.Driver").load()
    dft.registerTempTable(etsTempTable)
    ds_ets = sqlContext.sql(" select max(updatets) as max from %s " % (etsTempTable))
    pp = ds_ets.collect()[0]
    max_updates = pp.max
    slave_sql = ''
    try:
        if max_updates is not None:
            logging.info(u"ets库中的最大时间是：" + str(max_updates))
            slave_sql = " select id, schedule_id, std_id, updated_at " \
                        "  from  %s  where `updated_at` > '%s' " % (slaveTempTable, max_updates)
        else:
            logging.info(u"本次为初次抽取")
            slave_sql = " select id, schedule_id, std_id, updated_at " \
                        " from  %s  " % (slaveTempTable)
        ds_slave = sqlContext.sql(slave_sql)
        logging.info(u'slave 中 符合条件的记录数为：%s' % (ds_slave.count()))
        m = hashlib.md5()
        now_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logging.info(u'开始执行抽取数据...')
        for row in ds_slave.collect():
            src_fields = {
                'Schedule_Std': ['id', 'schedule_id', 'std_id', 'updated_at']}
            src_fields = json.dumps(src_fields)
            src_fieldsvul = "Schedule_Std.%s|Apply_Student.%s|Apply_Student.%s|Apply_Student.%s" \
                            % (row.id, row.schedule_id, row.std_id, row.updated_at)
            m.update(src_fieldsvul)
            src_fields_md5 = m.hexdigest()
            # Spark 2.2.0版本 可以直接使用 inert into values()
            # 下面的sql 兼容 spark 1.6 。 注意字段顺序要和 数据库完全一致
            sql = "insert into  %s  select A.* from (select '%s' as id," \
                  "'%s' as schedule_id ," \
                  "'%s' as std_id ," \
                  "'%s' as cust_no," \
                  "'%s' as isvalid," \
                  "'%s' as src_fields," \
                  "'%s' as src_fields_md5," \
                  "'%s' as createts," \
                  "'%s' as updatets ) A" \
                  % (etsTempTable, row.id, row.schedule_id, row.std_id,
                     cust_no, isvalid, src_fields, src_fields_md5,
                     now_time, row.updated_at)
            print u'打印sql@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@'
            print sql
            sqlContext.sql(sql)
        logging.info(u'抽取完成')
    # except Exception, e:
    #     # logging.error(e.message)
    #     #  Exception(e.message)
    finally:
        sc.stop()
