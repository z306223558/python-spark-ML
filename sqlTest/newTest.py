# -*- coding: UTF-8 -*-
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import datetime
import sys
import pymongo
import MySQLdb

date15 = pymongo.MongoClient(host="175.102.18.112", port=27017).shangHaiStudy.courseRecord.find(
    {"recordDate": {"$gt": datetime.datetime(2017, 04, 15, 0, 0, 0),"$lt": datetime.datetime(2017, 04, 15, 23, 59, 59)}})
date16 = pymongo.MongoClient(host="175.102.18.112", port=27017).shangHaiStudy.courseRecord.find(
    {"recordDate": {"$gt": datetime.datetime(2017, 04, 16, 0, 0, 0),"$lt": datetime.datetime(2017, 04, 16, 23, 59, 59)}})

date15Dir = {}
for date in date15:
    if date['mainTag'] not in date15Dir:
        date15Dir[date['mainTag']] = 0
    date15Dir[date['mainTag']] += date['learnNum']
print date15Dir

date16Dir = {}
for date in date16:
    if date['mainTag'] not in date16Dir:
        date16Dir[date['mainTag']] = 0
    date16Dir[date['mainTag']] += date['learnNum']
print date16Dir

sumbDir = {}
for k in date16Dir:
    sumbDir[k] = date16Dir[k] - date15Dir[k]

print sumbDir

