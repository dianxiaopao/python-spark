#!/usr/bin/env python
#-*- coding:UTF-8 -*-
from pyspark.sql import SparkSession

logFile = "/usr/local/spark-2.2.0-bin-hadoop2.7/README.md"  # Should be some file on your system
'''
官网是错的
SimpleApp.py第5行中的SparkSession.builder()
调用会失败，实际应该是SparkSession.builder.appName(appName).master(master).getOrCreate()。 
builder不是SparkSession的一个方法，而是一个属性
'''
spark = SparkSession.builder.appName('SimpleApp').master('local[*]').getOrCreate()
logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()
print ('##############################')
print("Lines with a: %i, lines with b: %i" % (numAs, numBs))
print ('##############################')
spark.stop()