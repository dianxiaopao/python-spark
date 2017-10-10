#!/usr/bin/env python
# coding=utf-8

"""
   author:zb
   create_at:2017-9-8 09:37:45
   涉及的学生人次，老师的人次
"""
import os
import sys
import datetime
import json
import traceback
from imp import load_source

from pyspark.sql import HiveContext


reload(sys)
sys.setdefaultencoding('utf-8')


def do_cs_task(sc, cs_dburl_env):
    # logger = setLog()
    cs_table = 'cs_person_count_comp'
    config = cs_dburl_env.get(cs_table, '')
    sqlContext = HiveContext(sc)
    driver = "com.mysql.jdbc.Driver"
    url_cs = config.get('dst', '')
    try:
        ets_table = load_source('get_which_for_cs', os.path.join(os.path.dirname(__file__), 'Utils.py')).get_which_for_cs(
            'task_ets_score', config.get('ets_score', ''))
        ets_score = sqlContext.read.format("jdbc").options(url=config.get('ets_score', ''), dbtable=ets_table,
                                                           driver=driver).load()
        ets_score.registerTempTable('ets_score')

        ets_table = load_source('get_which_for_cs',
                                os.path.join(os.path.dirname(__file__), 'Utils.py')).get_which_for_cs(
            'task_ets_learn', config.get('ets_score', ''))

        ets_learn = sqlContext.read.format("jdbc").options(url=config.get('ets_learn', ''), dbtable=ets_table,
                                                           driver=driver).load()
        ets_learn.registerTempTable('ets_learn')

        ets_table = load_source('get_which_for_cs',
                                os.path.join(os.path.dirname(__file__), 'Utils.py')).get_which_for_cs(
            'task_ets_osce_das_admin_report', config.get('ets_score', ''))

        ets_osce_das_admin_report = sqlContext.read.format("jdbc").options(url=config.get('ets_osce_das_admin_report', ''), dbtable=ets_table,
                                                                           driver=driver).load()
        ets_osce_das_admin_report.registerTempTable('ets_osce_das_admin_report')

        ets_table = load_source('get_which_for_cs',
                                os.path.join(os.path.dirname(__file__), 'Utils.py')).get_which_for_cs(
            'task_ets_tasks', config.get('ets_score', ''))

        ets_tasks = sqlContext.read.format("jdbc").options(url=config.get('ets_tasks', ''), dbtable=ets_table,
                                                           driver=driver).load()
        ets_tasks.registerTempTable('ets_tasks')

        ets_table = load_source('get_which_for_cs', os.path.join(os.path.dirname(__file__), 'Utils.py')).get_which_for_cs('task_ets_schedule_std', config.get('ets_score', ''))

        ets_schedule_std = sqlContext.read.format("jdbc").options(url=config.get('ets_schedule_std', ''), dbtable=ets_table,
                                                                  driver=driver).load()
        ets_schedule_std.registerTempTable('ets_schedule_std')

        ets_table = load_source('get_which_for_cs', os.path.join(os.path.dirname(__file__), 'Utils.py')).get_which_for_cs('task_ets_schedule_teacher', config.get('ets_score', ''))

        ets_schedule_teacher = sqlContext.read.format("jdbc").options(url=config.get('ets_schedule_teacher', ''), dbtable=ets_table,
                                                                      driver=driver).load()
        ets_schedule_teacher.registerTempTable('ets_schedule_teacher')

        ets_table = load_source('get_which_for_cs',
                                os.path.join(os.path.dirname(__file__), 'Utils.py')).get_which_for_cs(
            'task_ets_schedule', config.get('ets_score', ''))

        ets_schedule = sqlContext.read.format("jdbc").options(url=config.get('ets_schedule', ''), dbtable=ets_table,
                                                                      driver=driver).load()
        ets_schedule.registerTempTable('ets_schedule')

        ets_table = load_source('get_which_for_cs',
                                os.path.join(os.path.dirname(__file__), 'Utils.py')).get_which_for_cs(
            'task_ets_osce_das_admin_report', config.get('ets_score', ''))

        ets_osce_das_admin_report = sqlContext.read.format("jdbc").options(url=config.get('ets_osce_das_admin_report', ''), dbtable=ets_table,
                                                                           driver=driver).load()
        ets_osce_das_admin_report.registerTempTable('ets_osce_das_admin_report')

        ets_table = load_source('get_which_for_cs',
                                os.path.join(os.path.dirname(__file__), 'Utils.py')).get_which_for_cs(
            'task_ets_osce_das_admin_report', config.get('ets_score', ''))

        ets_osce_das_student_report = sqlContext.read.format("jdbc").options(url=config.get('ets_osce_das_student_report', ''), dbtable=ets_table,
                                                                             driver=driver).load()
        ets_osce_das_student_report.registerTempTable('ets_osce_das_student_report')

        ets_table = load_source('get_which_for_cs',
                                os.path.join(os.path.dirname(__file__), 'Utils.py')).get_which_for_cs(
            'task_ets_osce_das_examiner_report', config.get('ets_score', ''))

        ets_osce_das_examiner_report = sqlContext.read.format("jdbc").options(url=config.get('ets_osce_das_examiner_report', ''), dbtable=ets_table,
                                                                              driver=driver).load()
        ets_osce_das_examiner_report.registerTempTable('ets_osce_das_examiner_report')

        ets_table = load_source('get_which_for_cs',
                                os.path.join(os.path.dirname(__file__), 'Utils.py')).get_which_for_cs(
            'task_ets_questionsend', config.get('ets_score', ''))

        ets_questionsend = sqlContext.read.format("jdbc").options(url=config.get('ets_questionsend', ''), dbtable=ets_table,
                                                                  driver=driver).load()
        ets_questionsend.registerTempTable('ets_questionsend')

        ets_table = load_source('get_which_for_cs',
                                os.path.join(os.path.dirname(__file__), 'Utils.py')).get_which_for_cs(
            'task_ets_questionvoterecord', config.get('ets_score', ''))

        ets_questionvoterecord = sqlContext.read.format("jdbc").options(url=config.get('ets_questionvoterecord', ''), dbtable=ets_table,
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
        print(u"考站:%s" % stationcount)
        print(u"考官报告:%s" % stu_osce_examiner)
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

        dictssjosn = load_source('jsonTranfer', os.path.join(os.path.dirname(__file__), 'Utils.py')).jsonTranfer(dicts)
        temtuple = (1, dictssjosn, now_time, now_time)
        lists.append(temtuple)
        if len(lists) > 0:
            final_ds = sqlContext.createDataFrame(lists, ["id", "counts", "createts", "updatets"])
            print(final_ds.collect())
            # 删除表中数据 使用 jdbc方式
            ddlsql = " truncate table %s " % cs_table
            dbinfo = load_source('getdbinfo', os.path.join(os.path.dirname(__file__), 'Utils.py')).getdbinfo(url_cs)
            load_source('execute_sql_cs', os.path.join(os.path.dirname(__file__), 'Utils.py')).execute_sql_cs(ddlsql, dbinfo)
            final_ds.write.insertInto(cs_table)
        else:
            print(u'最终集合为空')
    except Exception, e:
        # e.message 2.6 不支持
        print (traceback.print_exc())
        print (str(e))
        raise Exception(str(e))

if __name__ == '__main__':
    pass
    # appname = 'rr_computer'
    # sc = SparkContext(appName=appname)
    # cp = getConfig()
    # cs_dburl_env = {"cs_person_count_comp": {
    #     "dst": cp.get('db', 'cs_url_all'),
    #     "ets_score": cp.get('db', 'ets_url_all'),
    #     "ets_gradeitem": cp.get('db', 'ets_url_all'),
    #     "ets_learn": cp.get('db', 'ets_url_all'),
    #     "ets_osce_das_admin_report": cp.get('db', 'ets_url_all'),
    #     "ets_osce_das_examiner_report": cp.get('db', 'ets_url_all'),
    #     "ets_osce_das_student_report": cp.get('db', 'ets_url_all'),
    #     "ets_osce_exam": cp.get('db', 'ets_url_all'),
    #     "ets_osce_exam_examinee": cp.get('db', 'ets_url_all'),
    #     "ets_osce_score": cp.get('db', 'ets_url_all'),
    #     "ets_osce_station": cp.get('db', 'ets_url_all'),
    #     "ets_questionsend": cp.get('db', 'ets_url_all'),
    #     "ets_questionvoterecord": cp.get('db', 'ets_url_all'),
    #     "ets_schedule": cp.get('db', 'ets_url_all'),
    #     "ets_schedule_std": cp.get('db', 'ets_url_all'),
    #     "ets_schedule_teacher": cp.get('db', 'ets_url_all'),
    #     "ets_score": cp.get('db', 'ets_url_all'),
    #     "ets_tasks": cp.get('db', 'ets_url_all'),
    #     "ets_apply_student": cp.get('db', 'ets_url_all')
    # }}
    # do_cs_task(sc, cs_dburl_env)

