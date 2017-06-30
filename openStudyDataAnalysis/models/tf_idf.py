# -*- coding: utf-8 -*-
import sys
sys.path.append("../")
from pyspark.ml.feature import Tokenizer, StopWordsRemover, IDF
from pyspark.ml.clustering import LDA
from pyspark.mllib.feature import HashingTF as MLH
from pyspark.mllib.util import MLUtils
from clustering import Clustering


class IDF_TO_LDA(Clustering):

    def __init__(self, ctx, df, params):
        super(IDF_TO_LDA, self).__init__(ctx, df, params)
        self.ctx = ctx
        self.df = df
        self.IDFTOLDAParams = self.params["clusteringParams"]


    def clustering(self):

        mlHashingTF = MLH()
        # 通过hashingTF提供的indexOf方法，获取单词和索引的映射关系，然后将对应关系广播到各节点
        mapWordsRdd = self.df.rdd.flatMap(lambda x: x["removeWords"]).map(lambda w: (mlHashingTF.indexOf(w), w))
        mapList = mapWordsRdd.collect()
        bdMapList = self.ctx.sparkContext.broadcast(mapList)

        # 特征转化，单词的向量形式转化
        hashingData = self.df.rdd.map(lambda x: (x, mlHashingTF.transform(x["removeWords"]))) \
            .toDF() \
            .toDF("words", "features")
        MLHashingData = MLUtils.convertVectorColumnsToML(hashingData, "features")

        # IDF算法调用，IDF的最终目的是去除多次重复出现在文本权重的影响，使得结果更加的平滑和客观
        idfModel = IDF(inputCol="hashingData",outputCol="features")
        model = idfModel.fit(MLHashingData)
        resultsData = model.transform(MLHashingData)

        # 计算完文本的IDF值，接下来我们将计算结果导入到LDA算法中去，然后使用LDA分析IDF的结果
        lda = LDA(k=self.IDFTOLDAParams["k"], maxIter=self.IDFTOLDAParams["maxIter"], optimizer=self.IDFTOLDAParams["optimizer"])
        topics = lda.fit(resultsData)
        results = topics.describeTopics(self.IDFTOLDAParams["describeTopics"])

        # 开始执行最后的数据结果输出，根据传递的参数，进行不同方式的输出

        def getWordsFromIndex(index):
            valueList = bdMapList.value
            nullStr = "null"
            for i in range(len(valueList)):
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

