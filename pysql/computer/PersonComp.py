#!/usr/bin/env python
# coding=utf-8

"""
   author:zb
   create_at:2017-9-8 09:37:45
   涉及的学生人次，老师的人次
"""
import hashlib
import os
import sys
import datetime
import json
import logging

from os import path

from pyspark.sql.types import StructField, StringType, StructType

from Utils import execute_sql_cs, setLog, getConfig

reload(sys)
sys.setdefaultencoding('utf-8')


from pyspark import SparkContext
from pyspark.sql import HiveContext


def computer():
    cs_table = 'cs_person_count_comp'
    appname = cs_table + '_computer'
    sc = SparkContext(appName=appname)
    sqlContext = HiveContext(sc)
    driver = "com.mysql.jdbc.Driver"
    url_ets = getConfig().get('db', 'ets_url_all')
    url_cs = getConfig().get('db', 'cs_url_all')
    try:
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

        ets_schedule = sqlContext.read.format("jdbc").options(url=url_ets, dbtable="ets_schedule",
                                                                      driver=driver).load()
        ets_schedule.registerTempTable('ets_schedule')

        ets_osce_das_admin_report = sqlContext.read.format("jdbc").options(url=url_ets, dbtable="ets_osce_das_admin_report",
                                                                           driver=driver).load()
        ets_osce_das_admin_report.registerTempTable('ets_osce_das_admin_report')

        ets_osce_das_student_report = sqlContext.read.format("jdbc").options(url=url_ets, dbtable="ets_osce_das_student_report",
                                                                             driver=driver).load()
        ets_osce_das_student_report.registerTempTable('ets_osce_das_student_report')

        ets_osce_das_examiner_report = sqlContext.read.format("jdbc").options(url=url_ets, dbtable="ets_osce_das_examiner_report",
                                                                              driver=driver).load()
        ets_osce_das_examiner_report.registerTempTable('ets_osce_das_examiner_report')

        ets_questionsend = sqlContext.read.format("jdbc").options(url=url_ets, dbtable="ets_questionsend",
                                                                              driver=driver).load()
        ets_questionsend.registerTempTable('ets_questionsend')

        ets_questionvoterecord = sqlContext.read.format("jdbc").options(url=url_ets, dbtable="ets_questionvoterecord",
                                                                  driver=driver).load()
        ets_questionvoterecord.registerTempTable('ets_questionvoterecord')

        # cs 数据库
        cs_person_count_comp = sqlContext.read.format("jdbc").options(url=url_cs, dbtable=cs_table,
                                                                              driver=driver).load()
        cs_person_count_comp.registerTempTable(cs_table)

        ets_questionsend_ds = sqlContext.sql(" select id, sg_id, type from ets_questionsend ")
        ets_questionvoterecord_ds = sqlContext.sql(" select id from ets_questionvoterecord ")

        ets_score_ds = sqlContext.sql(" select totalscore, operatorstudentid, graderstudentid, scoresheetcode from ets_score")
        ets_learn_ds = sqlContext.sql(" select  type, scoresheetcode  from ets_learn ")
        ets_task_ds = sqlContext.sql(" SELECT DISTINCT schedule_id from ets_tasks ")

        ets_schedule_std_ds = sqlContext.sql(" SELECT std_id, schedule_id from ets_schedule_std")

        ets_schedule_ds = sqlContext.sql(" SELECT id from ets_schedule")

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
        # 班级课后报告涉及的学生人次
        """
            QuestionSend
            
            
            班级课后报告涉及学生人次= 所有班级课后报告“上课总人数+全员回答学生参与人次+ 随机回答学生参与人次+ 抢答学生参与人次+投票学生参与人次”
            
            上课总人数：schedule_std count
            
            全员回答:QuestionSend 分组id为空 type = 0  
            
            随机回答学生参与人次:0 业务未记录
            
            抢答学生参与人次: QuestionSend type = 1  
            
            投票学生参与人次:QuestionVoteRecord 总记录数 
     
        """
        stu_bjkh = ets_schedule_std_ds.count() + ets_questionsend_ds.filter('sg_id is null and type = 0').count() \
                    + ets_questionsend_ds.filter('type = 1').count() + ets_questionvoterecord_ds.count()

        # osce 学生报告
        stu_osce_student = ets_osce_das_student_report_ds.count()
        # osce 管理者报告
        stu_osce_admin = stu_osce_student
        stationcount = 0
        for k in ets_osce_das_admin_report_ds.collect():
            stationarray = json.loads(k['report']).get('analysis').get('stationarray')
            stationcount = stationcount + len(stationarray)
        # osce 考官报告s涉及人数 =   考生数 * 考站数
        stationcount = (1 if stationcount == 0 else stationcount)
        stu_osce_examiner = stu_osce_student * stationcount
        logging.info(u"考站:%s" % stationcount)
        logging.info(u"考官报告:%s" % stu_osce_examiner)
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
        teacher_osce_admin = ets_osce_das_admin_report_ds.count()
        # osce 考官报告
        teacher_osce_examiner = ets_osce_das_examiner_report_ds.count()

        """
        涉及报告
        """
        # 在线训练涉及的报告数量
        report_zxxl = ets_learn_ds.filter(ets_learn_ds['type'] == 0).join(ets_score_ds, [ets_score_ds.scoresheetcode == ets_learn_ds.scoresheetcode]).count()
        # 班级课前训练涉及的报告数量
        report_bjkqxl = ets_task_ds.count()
        # 班级课堂训练报告涉及的报告数量
        report_bjktxl = ets_schedule_ds.count()
        # 同伴互助训练报告涉及的报告数量
        report_tbhz = ets_score_ds.filter('graderstudentid is not null').count()
        # 课上机器人(只能设备)训练报告涉及的报告数量
        report_ksznsb = ets_learn_ds.filter(ets_learn_ds['type'] == 2).join(ets_score_ds, [ets_score_ds.scoresheetcode == ets_learn_ds.scoresheetcode]).count()
        # 班级课后报告涉及的报告数量
        report_bjkh = ets_schedule_ds.count()
        # osce 学生报告
        report_osce_student = ets_osce_das_student_report_ds.count()
        # osce 管理者报告
        report_osce_admin = ets_osce_das_admin_report_ds.count()
        # osce 考官报告
        report_osce_examiner = ets_osce_das_examiner_report_ds.count()

        # print '_' * 50, '学生人次：班级课后训练、在线训练、班级课前训练、班级课堂训练 同伴互助训练，课上机器人，osce 学生报告，osce 管理者报告,osce 考官报告', '_' * 50
        #
        # print stu_bjkh, stu_zxxl, stu_bjkqxl, stu_bjktxl, stu_tbhz, stu_ksznsb, stu_osce_student, stu_admin_student, stu_osce_examiner
        # print '_' * 50, '老师人次：在线训练、班级课前训练、班级课堂训练 同伴互助训练，课上机器人，osce 学生报告，osce 管理者报告,osce 考官报告', '_' * 50
        #
        # print teacher_zxxl, teacher_bjkqxl, teacher_bjktxl, teacher_tbhz, teacher_ksznsb, teacher_bjkh, teacher_osce_examiner, teacher_admin_student, teacher_osce_student

        student_count = stu_bjkh + stu_zxxl + stu_bjkqxl + stu_bjktxl + stu_tbhz + stu_ksznsb \
                        + stu_osce_student + stu_osce_admin + stu_osce_examiner
        teacher_count = teacher_zxxl + teacher_bjkqxl + teacher_bjktxl + teacher_tbhz + teacher_ksznsb \
                        + teacher_bjkh + teacher_osce_examiner + teacher_osce_admin + teacher_osce_student
        report_count = report_zxxl + report_bjkqxl + report_bjktxl + report_tbhz + report_ksznsb \
                       + report_bjkh + report_osce_student + report_osce_admin+ report_osce_examiner
        print student_count, teacher_count, report_count

        now_time = datetime.datetime.now()
        # 存入数据库
        lists = []
        dicts = {"student": {"stu_bjkh": stu_bjkh, "stu_zxxl": stu_zxxl, "stu_bjkqxl": stu_bjkqxl, "stu_bjktxl": stu_bjktxl,
                             "stu_tbhz": stu_tbhz, "stu_ksznsb": stu_ksznsb, "stu_osce_student": stu_osce_student,
                             "stu_osce_admin": stu_osce_admin, "stu_osce_examiner": stu_osce_examiner, "student_count": student_count},
                 "teacher": {"teacher_zxxl": teacher_zxxl, "teacher_bjkqxl": teacher_bjkqxl, "teacher_bjktxl": teacher_bjktxl, "teacher_tbhz": teacher_tbhz,
                             "teacher_ksznsb": teacher_ksznsb, "teacher_bjkh": teacher_bjkh, "teacher_osce_examiner": teacher_osce_examiner,
                             "teacher_osce_admin": teacher_osce_admin, "teacher_osce_student": teacher_osce_student, "teacher_count": teacher_count},
                 "report": {"report_zxxl": report_zxxl, "report_bjkqxl": report_bjkqxl,
                            "report_bjktxl": report_bjktxl, "report_tbhz": report_tbhz,
                            "report_ksznsb": report_ksznsb, "report_bjkh": report_bjkh,
                            "report_osce_examiner": report_osce_examiner,
                            "report_osce_admin": report_osce_admin,
                            "report_osce_student": report_osce_student, "report_count": report_count}
                 }

        dictssjosn = json.dumps(dicts)
        temtuple = (1, dictssjosn, now_time, now_time)
        lists.append(temtuple)
        final_ds = sqlContext.createDataFrame(lists, ["id", "counts", "createts", "updatets"])
        logging.info(final_ds.collect())
        # 删除表中数据 使用 jdbc方式
        ddlsql = " truncate table %s " % cs_table
        execute_sql_cs(ddlsql)
        final_ds.write.insertInto(cs_table)
    except Exception, e:
        # e.message 2.6 不支持
        logging.error(str(e))
        raise Exception(str(e))

    finally:
        sc.stop()

if __name__ == '__main__':
    setLog()
    computer()


