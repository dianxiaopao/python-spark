# --coding:utf-8--
import ConfigParser
import os
import time
import json
import MySQLdb
from decimal import Decimal


# 自定义json转换器类
import logging

from os import path


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return obj.__str__()
        return json.JSONEncoder.default(self, obj)

'''
    配置信息
'''


def getConfig():
    cp = ConfigParser.SafeConfigParser()
    d = path.dirname(__file__)  # 返回当前文件所在的目录
    confpath = os.path.join(d, 'config.conf')
    cp.read(confpath)
    # global tomcatservicepath
    # tomcatservicepath = cp.get('path', 'tomcat-service')
    # global tomcatwebpath
    # tomcatwebpath = cp.get('path', 'tomcat-web')
    # global platformpath
    # platformpath = cp.get('path', 'platform')
    return cp

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


def jsonTranfer(strs):
    # ensure_ascii:禁止 中文汉字使用ascii编码  cls：自定义类型转换器
    return json.dumps(strs, cls=DecimalEncoder, ensure_ascii=False)


def TimeTranfer(timestamp):
    # 转换成localtime
    time_local = time.localtime(timestamp)
    # 转换成新的时间格式(2016-05-05 20:28:54)
    dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
    print dt


def execute_sql_ets(sql):
    setLog()
    cp = getConfig()
    connect = MySQLdb.connect(cp.get('db', 'ets_url'), cp.get('db', 'ets_user'), cp.get('db', 'ets_password'), cp.get('db', 'ets_db'), port=cp.getint('db', 'ets_port'), charset="utf8")
    cursor = connect.cursor()
    # SQL 插入语句
    try:
        # 执行sql语句
        cursor.execute(sql)
        # results = cursor.fetchall()
        # print results
        # 提交到数据库执行
        connect.commit()
    except Exception, e:
        # e.message 2.6 不支持
        connect.rollback()
        logging.error(str(e))
        raise Exception(str(e))
    # 关闭数据库连接
    connect.close()


def execute_sql_cs(sql):
    setLog()
    cp = getConfig()
    connect = MySQLdb.connect(cp.get('db', 'cs_url'), cp.get('db', 'cs_user'), cp.get('db', 'cs_password'),cp.get('db', 'cs_db'), port=cp.getint('db', 'cs_port'), charset="utf8")
    cursor = connect.cursor()
    # SQL 插入语句
    try:
        # 执行sql语句
        cursor.execute(sql)
        # results = cursor.fetchall()
        # print results
        # 提交到数据库执行
        connect.commit()
    except Exception, e:
        # e.message 2.6 不支持
        connect.rollback()
        logging.error(str(e))
        raise Exception(str(e))
    # 关闭数据库连接
    connect.close()
if __name__ == '__main__':
    execute_sql_cs("show tables")
    #TimeTranfer(1486667800)