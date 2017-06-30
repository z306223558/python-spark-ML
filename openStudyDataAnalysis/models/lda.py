# -*- coding: utf-8 -*-
import sys

from pyspark.mllib.util import MLUtils

sys.path.append("../")
from pyspark.mllib.feature import HashingTF as MLH
from pyspark.ml.clustering import LDA
from clustering import Clustering


class LDAClustering(Clustering):
    def __init__(self, ctx, df, params):
        super(LDAClustering, self).__init__(ctx, df, params)
        self.ctx = ctx
        self.df = df
        self.LDAParams = self.params["clusteringParams"]

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

        # 引入LDA，计算主题  LDA的输入必须是一个dataSet，而且该dataSet必须有一个features字段作为数据的输入
        # LDA的参数指定，其中k指示归档主题数目，maxIter指示最大迭代次数，次数越大则得出的结果越精准，optimizer是归档模式默认的em为本地归档，还有一个为 online模式待研究
        lda = LDA(k=self.LDAParams["k"], maxIter=self.LDAParams["maxIter"], optimizer=self.LDAParams["optimizer"])
        topics = lda.fit(MLHashingData)
        results = topics.describeTopics(self.LDAParams["describeTopics"])
        indices = results.select("topic").collect()
        termIndices = results.select("termIndices").collect()
        termWeights = results.select("termWeights").collect()

        if self.LDAParams["outWay"] == "console":
            for i in range(len(indices)):
                print "Topic_" + str(i) + ":"
                for j in range(len(termIndices[i]['termIndices'])):
                    print "" + self.getWordsFromIndex(termIndices[i]['termIndices'][j],bdMapList) + "   " + str(
                        termWeights[i]['termWeights'][j])

        if self.LDAParams["outWay"] == "textFile":
            import time
            results.saveAsTextFile(self.LDAParams["outFilePath"] + self.__class__ +"_" +str(time.time()) + ".txt")

        if self.LDAParams["outWay"] == "database":
            self.storeResultsToDataBase(results)
