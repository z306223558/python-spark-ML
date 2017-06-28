# -*- coding: UTF-8 -*-
import getopt
import json
import sys

import chardet

from util.SparkConnectionBase import SparkDBConnection

DEFAULT_DATA_CONF = dict(
    #集群相关配置
    sparkMaster="local[*]",
    appName="HUE_clustering",
    isLocal=True,
    sparkWarehousePath = sys.path[0] + "/spark-warehouse",
    #数据源相关配置
    inputType="mongodb",
    inputUri="mongodb://127.0.0.1:27017/",
    inputBase="openCourse",
    inputCollection="zhongwen",
    inputSqlScheme=dict(
        html="str",
        title="str"
    ),
    #数据输出配置
    outWay="console",
    outDataType="mongodb",
    outUri="mongodb://127.0.0.1:27017/",
    outbase="openCourseOut",
    outCollection="course_html_out",
    outSqlSchema=dict(
        results="str"
    )
)

DEFAULT_CLUSTER_CONF = dict(
    clustering="TEST",
    clusteringParams=dict(
        k=3,
        maxIter=20,
        optimizer="em",
        describeTopics=10
    ),
    #数据处理相关配置
    enableZH=True,
    allowNumber=False,
    limitCount=2,
    stopWords="责任编辑 本文 情况 显示 来源 企业 月份 公司 发现 查看 亿元 市场",
    defaultStopWords=[]
)


def main(argv):
    """程序主入口函数"""
    #直接使用全局变量，传递到后面的数据中去
    global DEFAULT_DATA_CONF
    global DEFAULT_CLUSTER_CONF

    try:
        options, args = getopt.getopt(argv, "c:", ["help","conf="])
    except getopt.GetoptError:
        print u"参数传递错误，请输入 --help 查看帮助"
        sys.exit()

    for option, value in options:
        if option in ("-h","--help"):
            print "本脚本执行参数如下：\n" \
                  "-h --help      查看帮助信息\n" \
                  "-c --conf      脚本配置信息，为固定格式的json字符串的Base64编码数据\n"\
                  "-s --setting   选择的数据处理算法的配置信息，比如：LDA TF-IDF等，包含自己所需要的数据"
        if option in ("-c","--conf"):
            #覆盖默认的参数
            try:
                conf = json.loads(value)
                if not isinstance(conf,dict):
                    print u"数据源信息：格式化后类型错误，请确保传入的参数信息为一个完整的json对象：{0}".format(value)
                    sys.exit()
                else:
                    DEFAULT_DATA_CONF = loadConf(conf,DEFAULT_DATA_CONF)
            except Exception as e:
                print u"数据源信息：格式化配置信息失败：{0}".format(value)
                sys.exit()
        if option in ("-s", "--setting"):
            #获取数据处理算法相关参数
            try:
                setting = json.loads(value)
                if not isinstance(setting,dict):
                    print u"算法信息：格式化后类型错误，请确保传入的参数信息为一个完整的json对象：{0}".format(value)
                    sys.exit()
                else:
                    DEFAULT_CLUSTER_CONF = loadConf(setting,DEFAULT_CLUSTER_CONF)
            except Exception as e:
                print u"算法信息：格式化配置信息失败：{0}".format(value)
                sys.exit()
    if len(args):
        print u"错误的配置信息为：{0}".format(args)


    # 实例化sparkDBConnection的基本连接类
    loader = SparkDBConnection(DEFAULT_DATA_CONF).loader
    # 获取到数据的spark.dataFrame
    dataFrame = loader.dbCache().DataFrame
    # 获取sparkSession示例
    spark = loader.getSparkSession()
    # 动态导入类
    modelName = "models"
    classFile = DEFAULT_CLUSTER_CONF["clustering"].lower()
    className = DEFAULT_CLUSTER_CONF['clustering'].upper() + "Clustering"
    importStr = "from " + modelName + "." + classFile + " import " + className + " as classObj"
    try:
        exec importStr
    except ImportError as e:
        print u"模块初始化失败，请检查传入的算法是否存在:"
        print e
        sys.exit()

    # print DEFAULT_CLUSTER_CONF
    DEFAULT_CLUSTER_CONF = loadStopWords(DEFAULT_CLUSTER_CONF)
    instanceObj = classObj(spark, dataFrame, DEFAULT_CLUSTER_CONF)
    instanceObj.clustering()



def loadConf(conf,confBase,type="cover"):
    """通用的2个字典属性覆盖的方法"""
    baseKeys = confBase.keys()
    loadKeys = conf.keys()
    for key in loadKeys:
        if type == 'cover':
            if key in baseKeys:
                confBase[key] = conf[key]
        if type == "append":
            confBase[key] = conf[key]
        if type == "short":
            if key in baseKeys:
                del confBase[key]
    return confBase

def loadStopWords(conf):
    f = open("data/stopWords_zh.txt", "r")
    lines = f.readlines()
    stopWords = []
    for line in lines:
        stopWords.append(line.strip().decode(chardet.detect(line.strip())["encoding"]))
    conf['defaultStopWords'] = conf['defaultStopWords'] + stopWords
    return conf

if __name__ == "__main__":
    main(sys.argv[1:])
