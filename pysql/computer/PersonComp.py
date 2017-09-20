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

    ets_tasks = sqlContext.read.format("jdbc").options(url=url_ets, dbtable="ets_tasks",
                                                       driver=driver).load()
    ets_tasks.registerTempTable('ets_tasks')

    ets_schedule_std = sqlContext.read.format("jdbc").options(url=url_ets, dbtable="ets_schedule_std",
                                                              driver=driver).load()
    ets_schedule_std.registerTempTable('ets_schedule_std')

    ets_schedule_teacher = sqlContext.read.format("jdbc").options(url=url_ets, dbtable="ets_schedule_teacher",
                                                                  driver=driver).load()
    ets_schedule_teacher.registerTempTable('ets_schedule_teacher')
    ets_osce_das_admin_report = sqlContext.read.format("jdbc").options(url=url_ets, dbtable="ets_osce_das_admin_report",
                                                                       driver=driver).load()
    ets_osce_das_admin_report.registerTempTable('ets_osce_das_admin_report')

    ets_osce_das_student_report = sqlContext.read.format("jdbc").options(url=url_ets, dbtable="ets_osce_das_student_report",
                                                                         driver=driver).load()
    ets_osce_das_student_report.registerTempTable('ets_osce_das_student_report')

    ets_osce_das_examiner_report = sqlContext.read.format("jdbc").options(url=url_ets, dbtable="ets_osce_das_examiner_report",
                                                                          driver=driver).load()
    ets_osce_das_examiner_report.registerTempTable('ets_osce_das_examiner_report')

    ets_score_ds = sqlContext.sql(" select totalscore, operatorstudentid, graderstudentid, scoresheetcode from ets_score")
    ets_learn_ds = sqlContext.sql(" select  type, scoresheetcode  from ets_learn ")
    ets_task_ds = sqlContext.sql(" SELECT DISTINCT schedule_id from ets_tasks ")

    ets_schedule_std_ds = sqlContext.sql(" SELECT std_id, schedule_id from ets_schedule_std")

    ets_schedule_teacher_ds = sqlContext.sql(" SELECT teacher_id, schedule_id from ets_schedule_teacher")
    ets_schedule_std_task_ds = ets_schedule_std_ds.join(ets_task_ds, ets_task_ds['schedule_id'] == ets_schedule_std_ds['schedule_id'], 'leftsemi')

    ets_osce_das_student_report_ds = sqlContext.sql("select report from ets_osce_das_student_report")

    ets_osce_das_admin_report_ds = sqlContext.sql("select report from ets_osce_das_admin_report")

    ets_osce_das_examiner_report_ds = sqlContext.sql("select report from ets_osce_das_examiner_report")

    """
    涉及学生  类型【0:在线训练，1:模型，2:智能设备，3:出科训练】
    """
    # 在线训练涉及的学生
    stu_zxxl = ets_learn_ds.filter(ets_learn_ds['type'] == 0).join(ets_score_ds, [ets_score_ds.scoresheetcode == ets_learn_ds.scoresheetcode]).count()
    # 班级课前训练涉及的学生人数
    stu_bjkqxl = ets_schedule_std_task_ds.count()
    # 班级课堂训练报告涉及的学生人次
    stu_bjktxl = ets_schedule_std_ds.count()
    # 同伴互助训练报告涉及的学生人次
    stu_tbhz = ets_score_ds.filter('graderstudentid is not null').count()
    # 课上机器人(只能设备)训练报告涉及的学生人次
    stu_ksznsb = ets_learn_ds.filter(ets_learn_ds['type'] == 2).join(ets_score_ds, [ets_score_ds.scoresheetcode == ets_learn_ds.scoresheetcode]).count()
    # 班级课后报告涉及的学生人次??????????????????????????????????
    stu_bjkh = 0

    # osce 学生报告
    stu_osce_student = ets_osce_das_student_report_ds.count()
    # osce 管理者报告
    stu_admin_student = ets_osce_das_admin_report_ds.count()
    # osce 考官报告
    stu_osce_examiner = ets_osce_das_examiner_report_ds.count()

    """
      涉及老师  
      """
    # 在线训练涉及的老师
    teacher_zxxl = ets_learn_ds.filter(ets_learn_ds['type'] == 0).join(ets_score_ds, [ets_score_ds.scoresheetcode == ets_learn_ds.scoresheetcode]).count()
    # 班级课前训练涉及的老师人数
    teacher_bjkqxl = ets_task_ds.count()
    # 班级课堂训练报告涉及的老师人次
    teacher_bjktxl = ets_schedule_teacher_ds.count()
    # 同伴互助训练报告涉及的老师人次
    teacher_tbhz = ets_score_ds.filter('graderstudentid is not null').count()
    # 课上机器人(只能设备)训练报告涉及的老师人次
    teacher_ksznsb = ets_learn_ds.filter(ets_learn_ds['type'] == 2).join(ets_score_ds, [ets_score_ds.scoresheetcode == ets_learn_ds.scoresheetcode]).count()
    # 班级课后报告涉及的老师人次??????????????????????????????????
    teacher_bjkh = ets_schedule_teacher_ds.count()
    # osce 学生报告
    teacher_osce_student = ets_osce_das_student_report_ds.count()
    # osce 管理者报告
    teacher_admin_student = ets_osce_das_admin_report_ds.count()
    # osce 考官报告
    teacher_osce_examiner = ets_osce_das_examiner_report_ds.count()

    print '_' * 50, '学生人次：在线训练、班级课前训练、班级课堂训练 同伴互助训练，课上机器人，osce 学生报告，osce 管理者报告,osce 考官报告', '_' * 50

    print stu_zxxl, stu_bjkqxl, stu_bjktxl, stu_tbhz, stu_ksznsb, stu_osce_student, stu_admin_student, stu_osce_examiner
    print '_' * 50, '老师人次：在线训练、班级课前训练、班级课堂训练 同伴互助训练，课上机器人，osce 学生报告，osce 管理者报告,osce 考官报告', '_' * 50

    print teacher_zxxl, teacher_bjkqxl, teacher_bjktxl, teacher_tbhz, teacher_ksznsb, teacher_bjkh, teacher_osce_examiner, teacher_admin_student, teacher_osce_student
    sc.stop()


if __name__ == '__main__':
    setLog()
    computer(2)


