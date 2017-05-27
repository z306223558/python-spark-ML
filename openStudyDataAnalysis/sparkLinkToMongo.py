# -*- coding: UTF-8 -*-
import sys

reload( sys )
sys.setdefaultencoding( 'utf-8' )

from util.SparkConnectionBase import SparkDBConnection
from models.categoryInfo import CategoryInfo
from models.tf_idf import TF_IDFTest
from models.lda import LDATest

if __name__ == "__main__":
    """程序运行总入口"""

    #设置全局的sparkmaster的地址和mongodb数据库的地址
    MAIN_HOST = '127.0.0.1'
    SPARK_HOST = 'local[*]'

    #开放大学的mongo数据导入配置
    ShangHaiOpenConf = dict(
        host=MAIN_HOST,
        database="shangHaiStudy",
        collection="courseRecord",
        appName="factoryClient",
        sparkMaster="175.102.18.112",
        sqlScheme={"name": "str", "id": "int",
                   "recordDate": "datetime"}
    )
    #世界开放大学mongo数据导入配置
    WorldOpenCourseConf = dict(
        host=MAIN_HOST,                             #数据库IP
        database="openCourse",                      #数据库名
        collection="college_html_back_2",           #mongo的collection名
        isLocal=False,                               #是服务器模式还是本地
        appName="sparkMongoToLDA",                          #sparkApp的名字
        sparkMaster="175.102.18.112",               #spark集群的Ip
        sqlScheme={"html": "str", "title": "str"}   #导入时的可选field的名字和类型
    )

    #实例化sparkmongo的基本连接类
    loader = SparkDBConnection("mongodb", WorldOpenCourseConf).loader
    #获取到数据的spark.dataFrame
    dataFrame = loader.dbCache().DateFrame
    #获取sparkSession示例
    spark = loader.getSparkSession
    #实例化各算法的示例类，调用对应的实现方法
    lda = LDATest(spark, dataFrame)
    lda.clustering()
