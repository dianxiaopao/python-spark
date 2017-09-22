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

from Utils import execute_sql_cs, jsonTranfer, setLog, getConfig

reload(sys)
sys.setdefaultencoding('utf-8')


from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import functions as F


def computer():
    cs_table = 'cs_person_score'
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

        ets_apply_student = sqlContext.read.format("jdbc").options(url=url_ets, dbtable="ets_apply_student",
                                                                   driver=driver).load()
        ets_apply_student.registerTempTable('ets_apply_student')

        # cs 数据库
        cs_person_score = sqlContext.read.format("jdbc").options(url=url_cs, dbtable=cs_table,
                                                                 driver=driver).load()
        cs_person_score.registerTempTable(cs_table)

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
        khxl_rank5_list = []
        for i in khxl_rank5:
            logging.info(i)
            logging.info(i.operatorstudentid)
            dicts = {}
            dicts['id'] = i.operatorstudentid
            dicts['score'] = i.totalscore
            khxl_rank5_list.append(dicts)

        khxl_rank5 = khxl_rank5_list
        #khxl_personcount = ets_apply_student_ds.count()

        # 在线训练  类型【0:在线训练，1:模型，2:智能设备，3:出科训练】
        type = 0
        zxxl_avgscore = ets_score_ds.filter(ets_score_ds['type'] == type).agg(F.avg(ets_score_ds['totalscore'])).collect()[0]['avg(totalscore)']
        zxxl_avgscore = (0 if zxxl_avgscore == None else zxxl_avgscore)

        zxxl_maxscore = ets_score_ds.filter(ets_score_ds['type'] == type).agg(F.max(ets_score_ds['totalscore'])).collect()[0]['max(totalscore)']
        zxxl_maxscore = (0 if zxxl_maxscore == None else zxxl_maxscore)

        zxxl_minscore = ets_score_ds.filter(ets_score_ds['type'] == type).agg(F.min(ets_score_ds['totalscore'])).collect()[0]['min(totalscore)']
        zxxl_minscore = (0 if zxxl_minscore == None else zxxl_minscore)
        # 排行榜
        zxxl_rank5 = ets_score_ds.filter(ets_score_ds['type'] == type).sort(ets_score_ds['totalscore'].desc()).take(5)
        zxxl_rank5_list = []
        for i in zxxl_rank5:
            dicts = {}
            dicts['id'] = i.operatorstudentid
            dicts['score'] = i.totalscore
            zxxl_rank5_list.append(dicts)
        zxxl_rank5 = zxxl_rank5_list
        #zxxl_personcount = ets_score_ds.filter(ets_score_ds['type'] == type).count()

        # 课上机器人
        type = 2
        znsb_avgscore = ets_score_ds.filter(ets_score_ds['type'] == type).agg(F.avg(ets_score_ds['totalscore'])).collect()[0]['avg(totalscore)']
        znsb_avgscore = (0 if znsb_avgscore == None else znsb_avgscore)

        znsb_maxscore = ets_score_ds.filter(ets_score_ds['type'] == type).agg(F.max(ets_score_ds['totalscore'])).collect()[0]['max(totalscore)']
        znsb_maxscore = (0 if znsb_maxscore == None else znsb_maxscore)

        znsb_minscore = ets_score_ds.filter(ets_score_ds['type'] == type).agg(F.min(ets_score_ds['totalscore'])).collect()[0]['min(totalscore)']
        znsb_minscore = (0 if znsb_minscore == None else znsb_minscore)
        # 排行榜
        znsb_rank5 = ets_score_ds.filter(ets_score_ds['type'] == type).sort(ets_score_ds['totalscore'].desc()).take(5)
        znsb_rank5_list = []
        for i in znsb_rank5:
            dicts = {}
            dicts['id'] = i.operatorstudentid
            dicts['score'] = i.totalscore
            znsb_rank5_list.append(dicts)
        znsb_rank5 = znsb_rank5_list
       # znsb_personcount = ets_score_ds.filter(ets_score_ds['type'] == type).count()

        # 课上模型人
        type = 1
        ksmxr_avgscore = ets_score_ds.filter(ets_score_ds['type'] == type).agg(F.avg(ets_score_ds['totalscore'])).collect()[0]['avg(totalscore)']
        ksmxr_avgscore = (0 if znsb_avgscore == None else znsb_avgscore)

        ksmxr_maxscore = ets_score_ds.filter(ets_score_ds['type'] == type).agg(F.max(ets_score_ds['totalscore'])).collect()[0]['max(totalscore)']
        ksmxr_maxscore = (0 if ksmxr_maxscore == None else ksmxr_maxscore)

        ksmxr_minscore = ets_score_ds.filter(ets_score_ds['type'] == type).agg(F.min(ets_score_ds['totalscore'])).collect()[0]['min(totalscore)']
        ksmxr_minscore = (0 if ksmxr_minscore == None else ksmxr_minscore)
        # 排行榜
        ksmxr_rank5 = ets_score_ds.filter(ets_score_ds['type'] == type).sort(ets_score_ds['totalscore'].desc()).take(5)
        ksmxr_rank5_list = []
        for i in ksmxr_rank5:
            dicts = {}
            dicts['id'] = i.operatorstudentid
            dicts['score'] = i.totalscore
            ksmxr_rank5_list.append(dicts)
        ksmxr_rank5 = ksmxr_rank5_list
        #ksmxr_personcount = ets_score_ds.filter(ets_score_ds['type'] == type).count()


        # osce 平均分，最高分，最低分，排行榜
        osce_maxscore = 0
        osce_minscore = 0
        osce_avgscore = 0
        osce_personcount = 0
        osce_rank5 =[]
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
            osce_rank5 = ets_osce_das_admin_report_new_ds.sort(ets_osce_das_admin_report_new_ds['score'].desc()).take(5)
            osce_rank5_list = []
            for i in osce_rank5:
                dicts = {}
                dicts['score'] = i.score
                dicts['name'] = i.name
                osce_rank5_list.append(dicts)
            osce_rank5 = osce_rank5_list
           # osce_personcount = ets_osce_das_admin_report_new_ds.count()

        # print '_' * 50, '在线训练、课上机器人、课上模型人 平均分，最高分，最低分，排行榜,参与人次', '_' * 50
        # print maxscore, minscore, avgscore, personcount,rank5
        # print '_' * 50, 'osce 平均分，最高分，最低分,排行榜,参与人次', '_' * 50
        # print osce_maxscore, osce_minscore, osce_avgscore, osce_personcount, osece_rank5
        # print '_' * 50, '课后训练 平均分，最高分，最低分,排行榜,参与人次', '_' * 50
        # print khxl_maxscore, khxl_minscore, khxl_avgscore, khxl_personcount, khxl_rank5

        now_time = datetime.datetime.now()
        # 存入数据库
        lists = []
        dicts = {"zxxl": {"zxxl_maxscore": zxxl_maxscore, "zxxl_minscore": zxxl_minscore, "zxxl_avgscore": zxxl_avgscore, "zxxl_rank5": zxxl_rank5},
                 "znsb": {"znsb_maxscore": znsb_maxscore, "znsb_minscore": znsb_minscore, "znsb_avgscore": znsb_avgscore, "znsb_rank5": znsb_rank5},
                 "ksmxr": {"ksmxr_maxscore": ksmxr_maxscore, "ksmxr_minscore": ksmxr_minscore, "ksmxr_avgscore": ksmxr_avgscore, "ksmxr_rank5": ksmxr_rank5},
                 "khxl_": {"khxl_maxscore": khxl_maxscore, "khxl_minscore": khxl_minscore, "khxl_avgscore": khxl_avgscore, "khxl_rank5": khxl_rank5},
                 "osce": {"osce_maxscore": osce_maxscore, "osce_minscore": osce_minscore, "osce_avgscore": osce_avgscore, "osce_rank5": osce_rank5}
                 }

        dictssjosn = jsonTranfer(dicts)
        temtuple = (1, dictssjosn, now_time, now_time)
        lists.append(temtuple)
        final_ds = sqlContext.createDataFrame(lists, ["id", "scores", "createts", "updatets"])
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


