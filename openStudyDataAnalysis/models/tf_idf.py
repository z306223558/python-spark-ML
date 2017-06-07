# -*- coding: utf-8 -*-
import sys

sys.path.append("../")
from pyspark.ml.feature import HashingTF as MH, Tokenizer, CountVectorizer, StopWordsRemover, IDF
from pyspark.ml.linalg import VectorUDT
from pyspark.ml.clustering import LDA
from pyspark.mllib.feature import HashingTF as MLH
from pyspark.mllib.util import MLUtils
import re


class TF_IDFTest():

    def __init__(self, ctx, df):
        self.ctx = ctx
        self.df = df


    def clustering(self):

        # 先将文本分词
        tokenizer = Tokenizer(inputCol="html", outputCol="words")
        self.df = tokenizer.transform(self.df)
        self.df = self.df.select("words")

        # 过滤数据
        # self.ctx.getContext().broadcast(self.filterWords)
        # self.df = self.df.rdd.map(lambda x: x["words"]).zipWithIndex().map(lambda y:(y[1],y[0])).mapValues(lambda w:w)
        # print self.df.take(2)

        # 过滤掉无用值
        remover = StopWordsRemover(inputCol="words", outputCol="removeWords")
        self.df = remover.transform(self.df)

        mlHashingTF = MLH()
        hashingData = self.df.rdd.map(lambda x:(x, mlHashingTF.transform(x[0]))).toDF().select("_1.removeWords","_2").toDF("words","hashingData")

        mapWords = hashingData.rdd.flatMap(lambda x: x["words"]).map(lambda w: (mlHashingTF.indexOf(w), w))
        mapList = list(set(mapWords.collect()))
        bdMapList = self.ctx.sparkContext.broadcast(mapList)

        # print mapList

        # # 引入IDF，计算主题
        idfModel = IDF(inputCol="hashingData",outputCol="features")
        MLHashingData = MLUtils.convertVectorColumnsToML(hashingData,"hashingData")
        model = idfModel.fit(MLHashingData)
        resultsData = model.transform(MLHashingData)

        #计算完文本的IDF值，接下来我们将计算结果导入到LDA算法中去，然后使用LDA分析IDF的结果
        # 引入LDA，计算主题
        lda = LDA(k=5, maxIter=10, optimizer="em")
        topics = lda.fit(resultsData)
        results = topics.describeTopics(5).cache()

        def getWordsFromIndex(index):
            #循环遍历bdMapList
            valueList = bdMapList.value
            nullStr = "null"
            for i in range(len(mapList)):
                if mapList[i][0] == index:
                    return mapList[i][1]
            return nullStr

        resultsList = results.collect()
        # 循环打印数据
        for i in range(len(resultsList)):
            print "Topic_" + str(resultsList[i]["topic"]) + ":"
            for j in range(len(resultsList[i]['termIndices'])):
                print "" + getWordsFromIndex(resultsList[i]["termIndices"][j]) + "   " + str(
                    resultsList[i]['termWeights'][j])

