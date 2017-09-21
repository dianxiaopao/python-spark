#!/usr/bin/env python
# coding=utf-8

"""
   author:zb
   create_at:2017-9-8 09:37:45
   进阶曲线计算
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
from pyspark.sql import functions as F


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


def computer(type):
    appname = 'score' + '_computer'
    sc = SparkContext(appName=appname)
    sqlContext = HiveContext(sc)
    driver = "com.mysql.jdbc.Driver"
    url_ets = 'jdbc:mysql://192.168.1.200:3307/bd_ets?user=root&password=13851687968' \
              '&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull'

    url_cs = 'jdbc:mysql://192.168.1.200:3309/bd_cs?user=root&password=13851687968' \
              '&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull'

    try:

        ets_score = sqlContext.read.format("jdbc").options(url=url_ets, dbtable="ets_score",
                                                           driver=driver).load()
        ets_score.registerTempTable('ets_score')

        ets_osce_das_admin_report = sqlContext.read.format("jdbc").options(url=url_ets, dbtable="ets_osce_das_admin_report",
                                                                           driver=driver).load()
        ets_osce_das_admin_report.registerTempTable('ets_osce_das_admin_report')

        ets_learn = sqlContext.read.format("jdbc").options(url=url_ets, dbtable="ets_learn",
                                                           driver=driver).load()
        ets_learn.registerTempTable('ets_learn')

        ets_osce_score = sqlContext.read.format("jdbc").options(url=url_ets, dbtable="ets_osce_score",
                                                                driver=driver).load()
        ets_osce_score.registerTempTable('ets_osce_score')

        ets_apply_student = sqlContext.read.format("jdbc").options(url=url_ets, dbtable="ets_apply_student",
                                                                   driver=driver).load()
        ets_apply_student.registerTempTable('ets_apply_student')

        # cs 数据库
        cs_person_advanced_curvecomp = sqlContext.read.format("jdbc").options(url=url_cs, dbtable="cs_person_advanced_curvecomp",
                                                                              driver=driver).load()
        cs_person_advanced_curvecomp.registerTempTable('cs_person_advanced_curvecomp')



        ets_osce_score_ds = sqlContext.sql("select * from ets_osce_score")
        # osce成绩
        ets_osce_score_group_ds = ets_osce_score_ds.groupBy(['examid', 'examineeid']).agg(F.sum(ets_osce_score_ds['totalscore']))
        # 课后训练
        ets_apply_student_ds = sqlContext.sql("select s.totalscore as apply_student_totalscore, s.operatorstudentid as operatorstudentid from ets_score s, ets_apply_student eas "
                                              "where s.operatorstudentid = eas.std_id and eas.status = 1 and s.starttime  >= FROM_UNIXTIME(eas.start_dt) and s.endtime<= FROM_UNIXTIME(eas.end_dt) ")
        # 在线训练、课上机器人、课上模型人  类型【0:在线训练，1:模型，2:智能设备，3:出科训练】
        ets_score_ds = sqlContext.sql("select s.totalscore as totalscore, s.operatorstudentid as operatorstudentid, l.type as type from ets_score s, ets_learn l "
                                      "where s.scoresheetcode = l.scoresheetcode ")
        zxxl_ds = ets_score_ds.filter(ets_score_ds['type'] == 0)
        # 按照人分组，取每个人的最大值 重命名   # 字段别名 展示用ksmxr_ds.select(ksmxr_ds.operatorstudentid.alias('operatorstudentid_id'))
        # df.withColumnRenamed 修改 df里面的列名，永久
        zxxl_ds = zxxl_ds.groupBy(['operatorstudentid']).agg(F.max(zxxl_ds['totalscore'])).withColumnRenamed('max(totalscore)', 'zxxl_totalscore')

        znsb_ds = ets_score_ds.filter(ets_score_ds['type'] == 2)
        znsb_ds = znsb_ds.groupBy(['operatorstudentid']).agg(F.max(znsb_ds['totalscore'])).withColumnRenamed('max(totalscore)', 'znsb_totalscore')

        ksmxr_ds = ets_score_ds.filter(ets_score_ds['type'] == 1)
        ksmxr_ds = ksmxr_ds.groupBy(['operatorstudentid']).agg(F.max(ksmxr_ds['totalscore'])).withColumnRenamed('max(totalscore)', 'ksmxr_totalscore')

        zxxl_znsb_ds = zxxl_ds.join(znsb_ds, 'operatorstudentid')
        zxxl_znsb_ksmxr_ds = zxxl_znsb_ds.join(ksmxr_ds, 'operatorstudentid')
        zxxl_znsb_ksmxr_ets_apply_student_ds = zxxl_znsb_ksmxr_ds.join(ets_apply_student_ds, 'operatorstudentid')
        all_ts = zxxl_znsb_ksmxr_ets_apply_student_ds.join(ets_osce_score_group_ds, ets_osce_score_group_ds['examineeid'] == zxxl_znsb_ksmxr_ets_apply_student_ds['operatorstudentid'])
        print '_' * 50
        logging.info(u'osce')
        logging.info(ets_osce_score_group_ds.collect())
        logging.info(u'课后训练')
        logging.info(ets_apply_student_ds.collect())
        logging.info(u'在线训练')
        logging.info(zxxl_ds.collect())
        logging.info(u'课上机器人')
        logging.info(znsb_ds.collect())
        logging.info(u'课上模型人')
        logging.info(ksmxr_ds.collect())
        logging.info(u'交集zxxl_znsb_ds')
        logging.info(zxxl_znsb_ds.collect())
        logging.info(u'交集zxxl_znsb_ksmxr_ds')
        logging.info(zxxl_znsb_ksmxr_ds.collect())
        logging.info(u'交集zxxl_znsb_ksmxr_ets_apply_student_ds')
        logging.info(zxxl_znsb_ksmxr_ets_apply_student_ds.collect())
        logging.info(u'最终交集')
        logging.info(all_ts.collect())
        logging.info(u'_' * 50)
        print '_' * 50
        now_time = datetime.datetime.now()
        # 存入数据库
        lists = []

        for index, k in enumerate(all_ts.collect()):
            # 拼接json
            """
            [Row(operatorstudentid=159796, zxxl_totalscore=Decimal('86.500'), znsb_totalscore=Decimal('56.500'), 
            ksmxr_totalscore=Decimal('32.000'), apply_student_totalscore=Decimal('82.000'), examid=1, examineeid=159796, 
            sum(totalscore)=Decimal('100.000'))]
            """
            dicts = {"kqselftraining":str(k['zxxl_totalscore']), "ksrobottraining":str(k['znsb_totalscore']), "ksmodeltraining":str(k['ksmxr_totalscore']),
                     "khselftraining":str(k['apply_student_totalscore']), "osce":str(k['sum(totalscore)'])}
            dictssjosn = json.dumps(dicts)
            temtuple = (index+1, k['operatorstudentid'], dictssjosn, now_time, now_time)
            lists.append(temtuple)
        final_ds = sqlContext.createDataFrame(lists, ["id", "student_id", "scores", "createts", "updatets"])
        logging.info(final_ds.collect())
        print final_ds.collect()
        final_ds.write.insertInto('cs_person_advanced_curvecomp')

    except Exception, e:
        # e.message 2.6 不支持
        logging.error(str(e))
        raise Exception(str(e))
    finally:
        sc.stop()
if __name__ == '__main__':
    setLog()
    computer(2)


