#-*- coding:UTF-8 -*-
#!/usr/bin/env python
from __future__ import print_function
import MySQLdb
from pyspark.sql import SparkSession
import  sys
reload(sys)
sys.setdefaultencoding('utf-8')
# 打开数据库连接
db = MySQLdb.connect("192.168.32.1", "root", "root", "testdjango", charset="utf8")
def main():
    spark = SparkSession.builder.appName('SimpleApp').master('local[*]').getOrCreate()
    url = "jdbc:mysql://192.168.32.1:3306/testdjango";
    table1 = "t_userinfo";
    dicts={}
    dicts['user'] = 'root'
    dicts['password'] = 'root'
    dicts["driver"] = "com.mysql.jdbc.Driver"
    dicts["charset"] = "utf8"
    jdbcDF1 = spark.read \
        .jdbc(url, table1, properties=dicts)
    print('开始执行printSchema')
    jdbcDF1.printSchema()
    print ('开始执行show')
    jdbcDF1.show()

    # 注册DataFrame为一个全局的SQL临时视图

    '''
    *Spark
    SQL的临时视图是当前session有效的，也就是视图会与创建该视图的session终止而失效。如果需要一个跨session而且一直有效的直到Spark应用终止才失效的临时视图，
    *可以使用全局临时视图。全局临时视图是与系统保留数据库global_temp绑定，
    *所以使用的时候必须使用该名字去引用，
    *例如，SELECT * FROM
    global_temp.view1。
   '''
    jdbcDF1.createOrReplaceTempView("userinfo")
    print("开始执行select 语句")
    sqlDF = spark.sql("SELECT * FROM userinfo where password='12343'")
    sqlDF.show()
    # 第二个表写
    table2 = "t_userinfo2"
    dicts["dbtable"] = table2 # 设置表
    jdbcDF2 = spark.read.jdbc(url, table2,  properties=dicts)
    print("打印ountput jdbcDF1.foreach()")
    jdbcDF1.foreach(f)

def f(obj):
    print(obj.username)
    sql = """insert into t_userinfo2 values
              (%s,'%s','%s') """ %(obj.id,obj.username,obj.password)
    insert(sql)

def insert(sql):
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
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
    main()