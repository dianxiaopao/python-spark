# --coding:utf-8--
import time

import MySQLdb


def TimeTranfer(timestamp):
    # 转换成localtime
    time_local = time.localtime(timestamp)
    # 转换成新的时间格式(2016-05-05 20:28:54)
    dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
    print dt


def execute_sql(sql):
    # 使用cursor()方法获取操作游标
    connect = MySQLdb.connect("192.168.1.200", "root", "13851687968", "bd_ets", port=3307, charset="utf8")
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
        print(str(e))
        raise Exception(str(e))
    # 关闭数据库连接
    connect.close()
if __name__ == '__main__':
    # execute_sql("show tables")
    TimeTranfer(1486667800)
    pass