# -*- coding: utf-8 -*-
import sys

from pyspark.mllib.util import MLUtils

sys.path.append("../")
from pyspark.mllib.feature import HashingTF as MLH
from pyspark.ml.clustering import LDA
from clustering import Clustering


class TESTClustering(Clustering):
    def __init__(self, ctx, df, params):
        super(TESTClustering, self).__init__(ctx, df, params)
        self.ctx = ctx


    def clustering(self):

        mlHashingTF = MLH()
        #得到单词和index的对应关系
        mapWordsRdd = self.df.rdd.flatMap(lambda x : x["words"]).map(lambda w: (mlHashingTF.indexOf(w),w))
        mapList = mapWordsRdd.collect()
        bdMapList = self.ctx.sparkContext.broadcast(mapList)

        #hashingData
        hashingData = self.df.rdd.map(lambda x:(x, mlHashingTF.transform(x["words"]))) \
            .toDF() \
            .toDF("words","features")
        MLHashingData = MLUtils.convertVectorColumnsToML(hashingData,"features")


        # 引入LDA，计算主题
        lda = LDA(k=3, maxIter=10, optimizer="em")
        topics = lda.fit(MLHashingData)
        results = topics.describeTopics(20).cache()
        indices = results.select("topic").collect()
        termIndices = results.select("termIndices").collect()
        termWeights = results.select("termWeights").collect()

        # 循环打印数据
        for i in range(len(indices)):
            print "Topic_" + str(i) + ":"
            for j in range(len(termIndices[i]['termIndices'])):
                try:
                    print "" + self.getWordsFromIndex(termIndices[i]['termIndices'][j],bdMapList) + "   " + str(
                        termWeights[i]['termWeights'][j])
                except Exception as e:
                    print e
                    pass
