#!/usr/bin/env python
#-*-coding:utf-8-*-
import random

import MySQLdb

def insert():
    # 打开数据库连接
    db = MySQLdb.connect("192.168.32.1", "root", "root", "testdjango", charset="utf8")
    # 使用cursor()方法获取操作游标
    seed=['M','F']
    cursor = db.cursor()
    genderrandom = (random.choice(seed))
    sql = """ insert into t_userinfo3 (`gender`,`height`) VALUES ('%s',%s)""" % (genderrandom,random.randint(170,190))
    # SQL 插入语句
    try:
        # 执行sql语句
        cursor.execute(sql)
        # 提交到数据库执行
        db.commit()
    except  Exception, e:
        print(e)
        db.rollback()
    # 关闭数据库连接
    db.close()

if __name__=='__main__':
    for i in range(100):
        insert()