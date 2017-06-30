# -*- coding: utf-8 -*-
import sys

import re
from pyspark import Row

sys.path.append("../")
from pyspark.ml.feature import Tokenizer, StopWordsRemover
import jieba


class Clustering(object):
    LIMIT_COUNT = 2
    STOPWORDS = []

    def __init__(self, ctx, df, params):
        # 初始化部分参数
        self.params = params
        self.LIMIT_COUNT = self.params["limitCount"]
        self.STOPWORDS = self.params["defaultStopWords"]
        self.jiebaObj = jieba
        self.spark = ctx
        """这里来进行指定的参数过滤方式"""
        # 第0步，我要先过滤掉特殊字符
        df = self.filterSpecailChar(df)
        # 第一步，分词（根据参数是否是中文选择不同的分词函数）
        if self.params["enableZH"]:
            df = self.jiebaCut(df)
        else:
            df = self.tokenizeRDD(df)
        # 第二步，过滤系统默认停用词
        df = self.stopWords(df)
        if self.params["enableZH"]:
            # 这里要一个中文停用词库
            df = self.stopDefaultWords(df)
        # 第三步，过滤用户自定义过滤词
        if self.params["stopWords"]:
            df = self.stopUserWords(df)
        # 第四步，根据用户配置是否过滤纯数字
        if not self.params["allowNumber"]:
            df = self.filterNumber(df)
        # 第五步，根据用户配置，确定过滤字数小于指定数目的单词
        self.df = self.filterLength(df)

    def filterSpecailChar(self, df):
        def filterZHChar(str):
            r = u'[a-zA-Z0-9’!"#$%&\'()*+,-./:;<=>?@，\\：。?★、…【】《》？“”‘’！[\\]^_`{|}~]+'
            return re.sub(r, '', str)

        def filterENChar(str):
            r = u'[0-9’!"#$%&\'()*+,-./:;<=>?@，：。\\?★、…【】《》？“”‘’！[\\]^_`{|}~]+'
            return re.sub(r, " ", str)

        dfW = df.rdd.map(lambda x: x["html"]).map(lambda y: Row(html=filterZHChar(y))).toDF()
        return dfW

    def filterLength(self, df):
        """过滤字符长度小于自定数目的单词"""
        limitCount = self.LIMIT_COUNT
        def filterArrayLength(x):
            temp = []
            for w in x:
                if len(w) >= limitCount:
                    temp.append(w)
            return temp
        dfL = df.rdd.map(lambda x: x["words"]).map(lambda x: Row(words=filterArrayLength(x))).toDF()
        return dfL

    def filterNumber(self, df):
        """过滤纯数字或者数字和特殊字符组合"""
        def filterArrayNumber(x):
            temp = []
            for w in x:
                if not w.isdigit():
                    temp.append(w)
            return temp
        dfN = df.rdd.map(lambda x: x["words"]).map(lambda x: Row(words=filterArrayNumber(x))).toDF()
        return dfN

    def tokenizeRDD(self, df):
        """分词实现"""
        # 先使用分词库分词
        tokenizer = Tokenizer(inputCol="html", outputCol="words")
        dfT = tokenizer.transform(df)
        return dfT.select("words").toDF("words")

    def stopWords(self, df):
        """使用ml库中的df高级api，去掉单词中的停用词，包含了绝大多是的常见辅助词，然后也需要去停用用户指定出来的特殊词"""
        stopWordsObj = StopWordsRemover(inputCol="words", outputCol="filterWords")
        dfF = stopWordsObj.transform(df)
        return dfF.select("filterWords").toDF("words")

    def stopDefaultWords(self, df):
        """加载中文停用词典"""
        stopWords = self.STOPWORDS
        def removeUserWords(y):
            temp = []
            for w in y:
                if w not in stopWords:
                    temp.append(w)
            return temp
        dfUF = df.rdd.map(lambda x: x["words"]).map(lambda y: Row(words=removeUserWords(y))).toDF()
        return dfUF

    def stopUserWords(self, df):
        """再次调用stopWords，或者使用rdd的filter功能"""
        stopWordsObj = StopWordsRemover(inputCol="words", outputCol="userFilterWords",
                                        stopWords=self.params["stopWords"].split(" "))
        dfUF = stopWordsObj.transform(df)
        return dfUF.select("userFilterWords").toDF("words")

    def jiebaCut(self, df):
        """调用结巴分词，进行中文分词"""
        jiebaObj = self.jiebaObj
        def getJiebaCut(x):
            return list(jiebaObj.cut(x))
        dfJB = df.rdd.map(lambda x: Row(words=getJiebaCut(x["html"]))).toDF()
        return dfJB

    def getWordsFromIndex(self,index,bdMapList):
        #循环遍历bdMapList
        valueList = bdMapList.value
        nullStr = "null"
        for i in range(len(valueList)):
            if valueList[i][0] == index:
                return valueList[i][1]
        return nullStr

    def storeResultsToDataBase(self,results):
        if self.params["outDataType"] == "mongodb":
            results.write.format("com.mongodb.spark.sql")\
                .mode("overwrite")\
                .options(
                    uri=self.params["outUri"],
                    database=self.params["outbase"],
                    collection=self.params["outCollection"]
                ).load()


