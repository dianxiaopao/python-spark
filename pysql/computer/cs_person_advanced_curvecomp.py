#!/usr/bin/env python
# coding=utf-8

"""
   author:zb
   create_at:2017-9-8 09:37:45
   进阶曲线计算
"""
import sys
import datetime
import traceback

from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import functions as F


from Utils import execute_sql_cs, setLog, loadjson, getdbinfo, jsonTranfer, getConfig
from advanced_curvecomp_util import get_newlist

reload(sys)
sys.setdefaultencoding('utf-8')


def do_cs_task(sc, cs_dburl_env):
    #logger = setLog()
    cs_table = 'cs_person_advanced_curvecomp'
    cs_dburl_env_dict = loadjson(cs_dburl_env)
    config = cs_dburl_env_dict.get(cs_table, '')
    sqlContext = HiveContext(sc)
    driver = "com.mysql.jdbc.Driver"
    url_cs = config.get('dst', '')
    try:

        ets_score = sqlContext.read.format("jdbc").options(url=config.get('ets_score', ''), dbtable="ets_score",
                                                           driver=driver).load()
        ets_score.registerTempTable('ets_score')

        ets_osce_das_admin_report = sqlContext.read.format("jdbc").options(url=config.get('ets_osce_das_admin_report', ''), dbtable="ets_osce_das_admin_report",
                                                                           driver=driver).load()
        ets_osce_das_admin_report.registerTempTable('ets_osce_das_admin_report')

        ets_learn = sqlContext.read.format("jdbc").options(url=config.get('ets_learn', ''), dbtable="ets_learn",
                                                           driver=driver).load()
        ets_learn.registerTempTable('ets_learn')

        ets_osce_score = sqlContext.read.format("jdbc").options(url=config.get('ets_osce_score', ''), dbtable="ets_osce_score",
                                                                driver=driver).load()
        ets_osce_score.registerTempTable('ets_osce_score')

        ets_apply_student = sqlContext.read.format("jdbc").options(url=config.get('ets_apply_student', ''), dbtable="ets_apply_student",
                                                                   driver=driver).load()
        ets_apply_student.registerTempTable('ets_apply_student')

        # cs 数据库
        cs_person_advanced_curvecomp = sqlContext.read.format("jdbc").options(url=url_cs, dbtable=cs_table, driver= driver).load()
        cs_person_advanced_curvecomp.registerTempTable(cs_table)

        ets_osce_score_ds = sqlContext.sql("select * from ets_osce_score")
        # osce成绩
        ets_osce_score_group_ds = ets_osce_score_ds.groupBy(['examid', 'examineeid']).agg(F.sum(ets_osce_score_ds['totalscore']))
        # 课后训练
        ets_apply_student_ds = sqlContext.sql("select s.totalscore as apply_student_totalscore, s.operatorstudentid as operatorstudentid from ets_score s, ets_apply_student eas "
                                              "where s.operatorstudentid = eas.std_id and eas.status = 1 and s.starttime  >= FROM_UNIXTIME(eas.start_dt) and s.endtime<= FROM_UNIXTIME(eas.end_dt) ")
        ets_apply_student_ds = ets_apply_student_ds.groupBy(['operatorstudentid']).agg(F.max(ets_apply_student_ds['apply_student_totalscore'])).withColumnRenamed('max(apply_student_totalscore)', 'apply_student_totalscore')

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

        ets_osce_score_group_ds = ets_osce_score_group_ds.withColumnRenamed('examineeid', 'operatorstudentid')
        # 输出方便 检查五个类型中是否有数据
        print(u'osce')
        print(ets_osce_score_group_ds.collect())
        print(u'课后训练')
        print(ets_apply_student_ds.collect())
        print(u'在线训练')
        print(zxxl_ds.collect())
        print(u'课上机器人')
        print(znsb_ds.collect())
        print(u'课上模型人')
        print(ksmxr_ds.collect())

        dslist = get_newlist(zxxl_ds, znsb_ds, ksmxr_ds, ets_apply_student_ds, ets_osce_score_group_ds)
        #print(dslist)
        print(u'长度'+str(len(dslist)))
        now_time = datetime.datetime.now()
        # 存入数据库
        lists = []
        i= 0
        for d in dslist:
            print(d.collect())
            for k in d.collect():
                # 有几个分数有值的，用作过滤
                sumval = 0
                i= i+1
                k = k.asDict()
                # 拼接json
                """
                [Row(operatorstudentid=159796, zxxl_totalscore=Decimal('86.500'), znsb_totalscore=Decimal('56.500'), 
                ksmxr_totalscore=Decimal('32.000'), apply_student_totalscore=Decimal('82.000'), examid=1, examineeid=159796, 
                sum(totalscore)=Decimal('100.000'))]
                """

                zxxl_totalscore = k.get('zxxl_totalscore','')
                znsb_totalscore = k.get('znsb_totalscore', '')
                ksmxr_totalscore = k.get('ksmxr_totalscore', '')
                apply_student_totalscore = k.get('apply_student_totalscore', '')
                totalscore = k.get('sum(totalscore)', '')

                if zxxl_totalscore:
                    sumval += 1
                if znsb_totalscore:
                    sumval += 1
                if ksmxr_totalscore:
                    sumval += 1
                if apply_student_totalscore:
                    sumval += 1
                if totalscore:
                    sumval += 1

                dicts = {"kqselftraining":zxxl_totalscore, "ksrobottraining":znsb_totalscore, "ksmodeltraining":ksmxr_totalscore,
                         "khselftraining":apply_student_totalscore, "osce":totalscore}
                dictssjosn = jsonTranfer(dicts)
                temtuple = (i, k.get('operatorstudentid',''), dictssjosn, sumval, now_time, now_time)
                lists.append(temtuple)
        if len(lists) > 0:
            final_ds = sqlContext.createDataFrame(lists, ["id", "student_id", "scores", "flag", "createts", "updatets"])
            final_ds_group = final_ds.groupBy(['student_id']).agg(F.max(final_ds['flag'])).withColumnRenamed('max(flag)', 'flag')
            final_ds = final_ds.join(final_ds_group, ["student_id", "flag"])
            final_ds = final_ds.drop(final_ds.flag)
            print(final_ds.collect())
            # 删除表中数据 使用 jdbc方式
            dbinfo = getdbinfo(url_cs)
            ddlsql = " truncate table %s " % cs_table
            execute_sql_cs(ddlsql, dbinfo)
            final_ds.write.insertInto(cs_table)
        else:
            print(u'最终集合为空')
    except Exception, e:
        # e.message 2.6 不支持
        print (traceback.print_exc())
        print (str(e))
        raise Exception(str(e))


if __name__ == '__main__':
    appname = 'rr_computer'
    sc = SparkContext(appName=appname)
    cp = getConfig()
    cs_dburl_env = {"cs_person_advanced_curvecomp": {
        "dst": cp.get('db', 'cs_url_all'),
        "ets_score": cp.get('db', 'ets_url_all'),
        "ets_gradeitem": cp.get('db', 'ets_url_all'),
        "ets_learn": cp.get('db', 'ets_url_all'),
        "ets_osce_das_admin_report": cp.get('db', 'ets_url_all'),
        "ets_osce_das_examiner_report": cp.get('db', 'ets_url_all'),
        "ets_osce_das_student_report": cp.get('db', 'ets_url_all'),
        "ets_osce_exam": cp.get('db', 'ets_url_all'),
        "ets_osce_exam_examinee": cp.get('db', 'ets_url_all'),
        "ets_osce_score": cp.get('db', 'ets_url_all'),
        "ets_osce_station": cp.get('db', 'ets_url_all'),
        "ets_questionsend": cp.get('db', 'ets_url_all'),
        "ets_questionvoterecord": cp.get('db', 'ets_url_all'),
        "ets_schedule": cp.get('db', 'ets_url_all'),
        "ets_schedule_std": cp.get('db', 'ets_url_all'),
        "ets_schedule_teacher": cp.get('db', 'ets_url_all'),
        "ets_score": cp.get('db', 'ets_url_all'),
        "ets_tasks": cp.get('db', 'ets_url_all'),
        "ets_apply_student": cp.get('db', 'ets_url_all')
    }}
    do_cs_task(sc, jsonTranfer(cs_dburl_env))


