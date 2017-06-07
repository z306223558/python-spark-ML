# -*- coding: utf-8 -*-
import sys

from pyspark.mllib.util import MLUtils

sys.path.append("../")
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, CountVectorizer, StopWordsRemover
from pyspark.mllib.feature import HashingTF as MLH
from pyspark.ml.clustering import LDA


class LDATest():
    def __init__(self, ctx, df):
        self.ctx = ctx
        self.df = df

    def clustering(self):

        # 先将文本分词
        tokenizer = Tokenizer(inputCol="html", outputCol="words")
        self.df = tokenizer.transform(self.df)
        self.df = self.df.select("words")

        # 过滤掉无用值
        stopWords = [" "]
        remover = StopWordsRemover(inputCol="words", outputCol="removeWords")
        self.df = remover.transform(self.df)

        # 标记文本
        wordsWithIds = self.df.rdd.zipWithIndex().map(
            lambda x: (x[1], [y.replace(u"\u2019", "") for y in x[0]['removeWords']])).toDF().toDF("id", "words")
        wordsWithIds.cache()

        # 使用CountVector将文档转化为词频向量，导入到LDA算法计算
        # countVector = CountVectorizer(inputCol="words", outputCol="features", vocabSize=1000, minDF=2)
        # model = countVector.fit(wordsWithIds)
        # hashingData = model.transform(wordsWithIds)

        # # 发现使用CountVector不能产生一一对应的关系，只能使用hashing
        # hashingTF = HashingTF(inputCol="words", outputCol="features")
        # hashingData = hashingTF.transform(wordsWithIds).cache()

        mlHashingTF = MLH()
        hashingData = self.df.rdd.map(lambda x:(x, mlHashingTF.transform(x[0]))).toDF().select("_1.removeWords","_2").toDF("words","features")

        mapWords = hashingData.rdd.flatMap(lambda x: x["words"]).map(lambda w: (mlHashingTF.indexOf(w), w))
        mapList = mapWords.collect()
        bdMapList = self.ctx.sparkContext.broadcast(mapList)
        MLHashingData = MLUtils.convertVectorColumnsToML(hashingData,"features")


        # 引入LDA，计算主题
        lda = LDA(k=5, maxIter=50, optimizer="em")
        topics = lda.fit(MLHashingData)
        results = topics.describeTopics(20).cache()
        indices = results.select("topic").collect()
        termIndices = results.select("termIndices").collect()
        termWeights = results.select("termWeights").collect()
        # # 计算得出word和index的list，得出index和word的映射关系
        # wordsMap = hashingData.rdd.map(lambda x: (0, (x["words"], x["features"].indices.tolist()))).reduceByKey(
        #     lambda x, y: (x[0] + y[0], x[1] + y[1])).map(lambda x: (x[1][0], x[1][1])).toDF().cache()
        #
        # wordsMap.printSchema()
        # wordsList = wordsMap.select("_1").collect()[0]["_1"]
        # indexList = wordsMap.select("_2").collect()[0]["_2"]

        # def getWordsFromIndex(targetId):
        #     return wordsList[indexList.index(targetId)]

        def getWordsFromIndex(index):
            #循环遍历bdMapList
            valueList = bdMapList.value
            nullStr = "null"
            for i in range(len(mapList)):
                if mapList[i][0] == index:
                    return mapList[i][1]
            return nullStr

        # 循环打印数据
        for i in range(len(indices)):
            print "Topic_" + str(i) + ":"
            for j in range(len(termIndices[i]['termIndices'])):
                print "" + getWordsFromIndex(termIndices[i]['termIndices'][j]) + "   " + str(
                    termWeights[i]['termWeights'][j])
