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

    ets_score = sqlContext.read.format("jdbc").options(url=url_ets, dbtable="ets_score",
                                                       driver=driver).load()
    ets_score.registerTempTable('ets_score')

    ets_learn = sqlContext.read.format("jdbc").options(url=url_ets, dbtable="ets_learn",
                                                       driver=driver).load()
    ets_learn.registerTempTable('ets_learn')

    ets_osce_das_admin_report = sqlContext.read.format("jdbc").options(url=url_ets, dbtable="ets_osce_das_admin_report",
                                                                       driver=driver).load()
    ets_osce_das_admin_report.registerTempTable('ets_osce_das_admin_report')

    ets_apply_student = sqlContext.read.format("jdbc").options(url=url_ets, dbtable="ets_apply_student",
                                                               driver=driver).load()
    ets_apply_student.registerTempTable('ets_apply_student')

    ets_score_ds = sqlContext.sql("select s.totalscore as totalscore, s.operatorstudentid as operatorstudentid, l.type as type from ets_score s, ets_learn l "
                                  "where s.scoresheetcode = l.scoresheetcode ")
    # 课后自主训练 平均分，最高分，最低分,参与人次
    ets_apply_student_ds = sqlContext.sql("select s.totalscore as totalscore, s.operatorstudentid as operatorstudentid from ets_score s, ets_apply_student eas "
                                          "where s.operatorstudentid = eas.std_id and eas.status = 1 and s.starttime  >= FROM_UNIXTIME(eas.start_dt) and s.endtime<= FROM_UNIXTIME(eas.end_dt) ")

    khxl_avgscore = ets_apply_student_ds.agg(F.avg(ets_apply_student_ds['totalscore'])).collect()[0]['avg(totalscore)']
    khxl_avgscore = (0 if khxl_avgscore == None else khxl_avgscore)

    khxl_maxscore = ets_apply_student_ds.agg(F.max(ets_apply_student_ds['totalscore'])).collect()[0]['max(totalscore)']
    khxl_maxscore = (0 if khxl_maxscore == None else khxl_maxscore)

    khxl_minscore = ets_apply_student_ds.agg(F.min(ets_apply_student_ds['totalscore'])).collect()[0]['min(totalscore)']
    khxl_minscore = (0 if khxl_minscore == None else khxl_minscore)
    # 排行榜
    khxl_rank5 = ets_apply_student_ds.sort(ets_apply_student_ds['totalscore'].desc()).take(5)
    khxl_personcount = ets_apply_student_ds.count()

    # 在线训练、课上机器人、课上模型人 平均分，最高分，最低分
    avgscore = ets_score_ds.filter(ets_score_ds['type'] == type).agg(F.avg(ets_score_ds['totalscore'])).collect()[0]['avg(totalscore)']
    avgscore = (0 if avgscore == None else avgscore)

    maxscore = ets_score_ds.filter(ets_score_ds['type'] == type).agg(F.max(ets_score_ds['totalscore'])).collect()[0]['max(totalscore)']
    maxscore = (0 if maxscore == None else maxscore)

    minscore = ets_score_ds.filter(ets_score_ds['type'] == type).agg(F.min(ets_score_ds['totalscore'])).collect()[0]['min(totalscore)']
    minscore = (0 if minscore == None else minscore)
    # 排行榜
    rank5 = ets_score_ds.sort(ets_score_ds['totalscore'].desc()).take(5)
    personcount = ets_score_ds.count()
    # osce 平均分，最高分，最低分，排行榜
    osce_maxscore = 0
    osce_minscore = 0
    osce_avgscore = 0
    osce_personcount = 0
    osece_rank5 =[]
    ets_osce_das_admin_report_ds = sqlContext.sql("select report from ets_osce_das_admin_report")
    lists = []
    for k in ets_osce_das_admin_report_ds.collect():
        rankinglist = json.loads(k['report']).get('rankinglist')
        for i in rankinglist:
            # print i['ranking'], i['score'], i['name']
            temtuple = (i['ranking'], i['score'], i['name'])
            lists.append(temtuple)
    if len(lists) > 0:
        ets_osce_das_admin_report_new_ds = sqlContext.createDataFrame(lists, ["ranking", "score", "name"])
        osce_maxscore = ets_osce_das_admin_report_new_ds.agg(F.max(ets_osce_das_admin_report_new_ds['score'])).collect()[0]['max(score)']
        osce_maxscore = (0 if osce_maxscore == None else osce_maxscore)

        osce_minscore = ets_osce_das_admin_report_new_ds.agg(F.min(ets_osce_das_admin_report_new_ds['score'])).collect()[0]['min(score)']
        osce_minscore = (0 if osce_maxscore == None else osce_minscore)

        osce_avgscore = ets_osce_das_admin_report_new_ds.agg(F.avg(ets_osce_das_admin_report_new_ds['score'])).collect()[0]['avg(score)']
        osce_avgscore = (0 if osce_maxscore == None else osce_avgscore)
        osece_rank5 = ets_osce_das_admin_report_new_ds.sort(ets_osce_das_admin_report_new_ds['score'].desc()).take(5)
        osce_personcount = ets_osce_das_admin_report_new_ds.count()

    """
    涉及学生  类型【0:在线训练，1:模型，2:智能设备，3:出科训练】
    """
    # 在线训练涉及的学生
    stu_zxxl = ets_score_ds.filter(ets_score_ds['type'] == 0).count()

    # 在线训练涉及的学生
    stu_zxxl = ets_score_ds.filter(ets_score_ds['type'] == 1).count()

    # 在线训练涉及的学生
    stu_zxxl = ets_score_ds.filter(ets_score_ds['type'] == 2).count()

    # 在线训练涉及的学生
    stu_zxxl = ets_score_ds.filter(ets_score_ds['type'] == 3).count()















    print '_' * 50, '在线训练、课上机器人、课上模型人 平均分，最高分，最低分，排行榜,参与人次', '_' * 50
    print maxscore, minscore, avgscore, personcount,rank5
    print '_' * 50, 'osce 平均分，最高分，最低分,排行榜,参与人次', '_' * 50
    print osce_maxscore, osce_minscore, osce_avgscore, osce_personcount, osece_rank5
    print '_' * 50, '课后训练 平均分，最高分，最低分,排行榜,参与人次', '_' * 50
    print khxl_maxscore, khxl_minscore, khxl_avgscore, khxl_personcount, khxl_rank5
    sc.stop()


if __name__ == '__main__':
    setLog()
    computer(2)


