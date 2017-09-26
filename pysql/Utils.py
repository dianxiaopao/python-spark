# --coding:utf-8--
import ConfigParser
import os
import re
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
    #  这里进行判断，如果logger.handlers列表为空，则添加，否则，直接去写日志
    if not logger.handlers:
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
    return logger


def jsonTranfer(strs):
    # ensure_ascii:禁止 中文汉字使用ascii编码  cls：自定义类型转换器
    return json.dumps(strs, cls=DecimalEncoder, ensure_ascii=False)


def loadjson(strs):
    dt = json.loads(strs)
    # print dt
    return dt


def TimeTranfer(timestamp):
    # 转换成localtime
    time_local = time.localtime(timestamp)
    # 转换成新的时间格式(2016-05-05 20:28:54)
    dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
    print dt


def execute_sql_ets(sql, cp):
    connect = MySQLdb.connect(cp.get('url'), cp.get('user'), cp.get('password'), cp.get('db'), port=int(cp.get('port')), charset="utf8")
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
        print (str(e))
        raise Exception(str(e))
    # 关闭数据库连接
    connect.close()


def execute_sql_cs(sql, cp):
    connect = MySQLdb.connect(cp.get('url'), cp.get('user'), cp.get('password'), cp.get('db'), port=int(cp.get('port')), charset="utf8")
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
        print (str(e))
        raise Exception(str(e))
    # 关闭数据库连接
    connect.close()


def getdbuser(str):
    # jdbc:mysql://192.168.1.200:3307/bd_ets?user=root&password=13851687968&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull jdbc:mysql://192.168.1.200:3306/osce1030?user=root&password=misrobot_whu&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull
    regroup = re.search(r'.*user=(.*\w*)&password', str)
    if regroup:
        print regroup.group(1)
        return regroup.group(1).strip()
    else:
        return -1


def getdbinfo(str):
    # jdbc:mysql://192.168.1.200:3307/bd_ets?user=root&password=13851687968&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull
    regpassword = re.search(r'.*password=(.*\w*)&useUnicode', str)
    reguser = re.search(r'.*user=(.*\w*)&password', str)
    regdburl = re.search(r'.*//(.*\w*):', str)
    regdb = re.search(r'.*/(.*\w*)\?', str)
    regport = re.search(r'.*:(.*\w*)/', str)
    ret = {}
    if reguser:
        ret['user'] = reguser.group(1)
    if regpassword:
        ret['password'] = regpassword.group(1)
    if regdburl:
        ret['url'] = regdburl.group(1)
    if regdb:
        ret['db'] = regdb.group(1)
    if regport:
        ret['port'] = regport.group(1)
    return ret


if __name__ == '__main__':
    #execute_sql_cs("show tables")
    TimeTranfer(1486667450)
    # ets_dburl_env = {"ets_learn": {
    #     "src": "jdbc:mysql://192.168.1.200:3306/osce1030?user=root&password=misrobot_whu&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull",
    #     "dst": "jdbc:mysql://192.168.1.200:3307/bd_ets?user=root&password=13851687968&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull"}}
    #
    # do_ets_task("", jsonTranfer(ets_dburl_env), "")
    str = "jdbc:mysql://192.168.1.200:3307/bd_ets?user=root&password=13851687968&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull"
    print (getdbinfo(str))