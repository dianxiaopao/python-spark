#!/usr/bin/env python
# coding=utf-8

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


if __name__ == '__main__':
    setLog()
    # 定义客户标识
    cust_no = '1'
    sc = SparkContext(appName="ScoreInsert")
    sqlContext = HiveContext(sc)
    # driver = "com.mysql.jdbc.Driver"
    dff = sqlContext.read.format("jdbc").options(url="jdbc:mysql://192.168.1.200:3306/osce1030?user=root"
                                                     "&password=misrobot_whu&useUnicode=true&characterEncoding=UTF-8"
                                                     "&zeroDateTimeBehavior=convertToNull", dbtable="osce_score",
                                                     driver="com.mysql.jdbc.Driver").load()
    dff.registerTempTable('osce_score_slave')

    dft = sqlContext.read.format("jdbc").options(url="jdbc:mysql://192.168.1.200:3307/bd_ets?user=root"
                                                     "&password=13851687968&useUnicode=true&characterEncoding=UTF-8"
                                                     "&zeroDateTimeBehavior=convertToNull", dbtable="ets_osce_score",
                                                       driver="com.mysql.jdbc.Driver").load()
    dft.registerTempTable('ets_osce_score')
    ds_ets = sqlContext.sql(" select max(updatets) as max from ets_osce_score ")
    pp = ds_ets.collect()[0]
    max_updates = pp.max
    slave_sql = ''
    try:
        if max_updates is not  None:
            logging.info(u"ets库中的最大时间是：" + str(max_updates))
            slave_sql = " select id, examineeid, examid, roomid, stationid, examinerid, totalscore, begintime ,endtime,scoresheetcode,status" \
                        "  from slave_osce_score where `updatetime` >= '%s' " % (max_updates)
        else:
            logging.info(u"本次为初次抽取")
            slave_sql = " select id, examineeid, examid, roomid, stationid, examinerid, totalscore, begintime ,endtime,scoresheetcode,status " \
                        " from slave_osce_score  "
        ds_slave = sqlContext.sql(slave_sql)
        logging.info(u'slave 中 符合条件的记录数为：%s' %(ds_slave.count()))
        m = hashlib.md5()
        now_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logging.info(u'开始执行抽取数据...')
        for row in ds_slave.collect():
            src_fields = {'Score': ['id', 'examineeid', 'examid', 'roomid', 'stationid', 'examinerid', 'totalscore', 'begintime', 'endtime', 'scoresheetcode', 'status']}
            src_fields = json.dumps(src_fields)
            src_fieldsvul ="osce_score.%s|osce_score.%s|osce_score.%s|osce_score.%s|osce_score.%s|osce_score.%s|osce_score.%s|osce_score.%s" \
                           "|osce_score.%s|osce_score.%s|osce_score.%s" % (row.id, row.examineeid, row.examid, row.roomid, row.stationid, row.examinerid, row.totalscore, row.begintime, row.endtime, row.status)
            print src_fields
            print src_fieldsvul
            m.update(src_fieldsvul)
            src_fields_md5 = m.hexdigest()
            # Spark 2.2.0版本 可以直接使用 inert into values()
            # 下面的sql 兼容 spark 1.6 。
            sql = "insert into  ets_score  select A.* from (select '%s' as id," \
                  "'%s' as examineeid ," \
                  "'%s' as examid ," \
                  "'%s' as roomid," \
                  "'%s' as stationid," \
                  "'%s' as examinerid," \
                  "'%s' as totalscore," \
                  "'%s' as begintime," \
                  "'%s'as endtime ," \
                  "'%s' as scoresheetcode," \
                  "'%s' as status," \
                  "'%s' as src_fields," \
                  "'%s' as src_fields_md5," \
                  "'%s' as cust_no," \
                  "'%s' as isvalid," \
                  "'%s' as createts," \
                  "'%s' as updatets ) A"\
                  % (row.id, row.examineeid, row.examid, row.roomid, row.stationid,
                     row.examinerid, row.totalscore, row.begintime, row.endtime,
                     row.scoresheetcode, row.status,src_fields, src_fields_md5,
                     cust_no, '1',  now_time, row.updatetime)
            print u'打印sql@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@'
            print sql
            sqlContext.sql(sql)
        logging.info(u'抽取完成')
    # except Exception, e:
    #     # logging.error(e.message)
    #     #  Exception(e.message)
    finally:
        sc.stop()

