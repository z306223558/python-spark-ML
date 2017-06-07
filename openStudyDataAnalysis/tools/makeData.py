# -*- coding: UTF-8 -*-
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import datetime
import sys
import pymongo
import re

def multiple_replace(text, adict):
    rx = re.compile('|'.join(map(re.escape, adict)))
    def one_xlat(match):
        return adict[match.group(0)]
    return rx.sub(one_xlat, text)

def make_xlat(*args, **kwds):
    adict = dict(*args, **kwds)
    rx = re.compile('|'.join(map(re.escape, adict)))
    def one_xlat(match):
        return adict[match.group(0)]
    def xlat(text):
        return rx.sub(one_xlat, text)
    return xlat


text = "Larry Wall \xa0 \2019u is the creator of Perl"
adict = {
    "\xa0" : "",
    "\u2019s":"",
    "\2019u":"",
    "div>":"",
    "\xae":"",
    "\xc2":"",
    "\xe2":""
}
# print multiple_replace(text, adict)
# translate = make_xlat(adict)
# print translate(text)




db = pymongo.MongoClient(host="127.0.0.1", port=27017).openCourse.college_html_back
htmls = pymongo.MongoClient(host="127.0.0.1", port=27017).openCourse.college_html_back.find()

def filterStr(s):
    # try:
    filter_str = re.sub("[\d+©\s+\.\!\/_,$%^*(+\"\']+|[+——！:;；=?\-，。？、~@#￥%……&*（）\(\)\[\]]+".decode("utf-8"),
                        " ".decode("utf-8"),
                        unicode(s)).encode("utf-8","replace")
    #再去掉多余的" "
    strArr = filter_str.split(" ")
    temp = []
    for str in strArr:
        if str != " " and len(str) > 2:
            # str = multiple_replace(str.strip(),adict).encode()
            temp.append(str)
    return " ".join(temp)
    # except Exception as e:
    #     return False


for html in htmls:
    filterTitle = filterStr(html['title'])
    filterHtml = filterStr(html['html'])
    if filterHtml == False or filterTitle == False:
        continue
    else:
        print(db.update({"_id":html['_id']},{"$set":{"title":filterTitle,"html":filterHtml,"hasChange":True}}))

