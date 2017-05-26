# -*- coding: UTF-8 -*-
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import datetime
import sys
import pymongo
import re

db = pymongo.MongoClient(host="127.0.0.1", port=27017).openCourse.college_html
htmls = pymongo.MongoClient(host="127.0.0.1", port=27017).openCourse.college_html.find()

def filterStr(s):
    # try:
    filter_str = re.sub("[©\|\s+\.\!\/_,$%^*(+\"\']+|[+——！，。？、~@#￥%……&*（）\(\)\[\]]+".decode("utf-8"),
                        " ".decode("utf-8"),
                        unicode(s)).encode("utf-8","replace")
    #再去掉多余的" "
    strArr = filter_str.split(" ")
    temp = []
    for str in strArr:
        if str != " ":
            temp.append(str)
    return " ".join(temp).strip().replace(r"\u2019","").replace(r"\u201c","").replace(r"\u201d","")
    # except Exception as e:
    #     return False


for html in htmls:
    filterTitle = filterStr(html['title'])
    filterHtml = filterStr(html['html'])
    if filterHtml == False or filterTitle == False:
        continue
    else:
        print(db.update({"_id":html['_id']},{"$set":{"title":filterTitle,"html":filterHtml,"hasChange":True}}))

