# -*- coding: UTF-8 -*-
import sys

reload( sys )
sys.setdefaultencoding( 'utf-8' )
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from util import SparkConnectionBase


oracleDict = dict(
    host="175.102.18.53", port=1521, table="STUDENT",
    appName="oracleConnection", sparkMaster="175.102.18.112",
    user="scott", password="tiger",
    sparkSC=None
)

studentConn = SparkConnectionBase.SparkDBConnection( "oracle", oracleDict)
studentDF = studentConn.loader.dbCache( ).DateFrame
sc = studentConn.loader.SparkInc
spark = studentConn.loader.SparkSQLInc
#设置表和sc
oracleDict['sparkSC'] = sc
oracleDict['table'] = "SCORE"

scoreDF = SparkConnectionBase.SparkDBConnection( "oracle", oracleDict).loader.dbCache( ).DateFrame
scoreDF.printSchema()

# # 首先是创建conf对象,这里会制定app的名字和master的位置，如果默认为本机则 使用local，若制定为 local[2]表示2个节点的模拟
# conf = SparkConf().setAppName("sqlTest").setMaster('spark://175.102.18.112:7077')
# # 创建sparkContext对象
# # sc = SparkContext(conf=conf)
# ####sparkSession 详解
# # conf可以自定义需要设置的参数值
# # 还有一个函数 .enableHiveSupport()使用后可以支持 Hive的功能
# # getOrCreate 函数会返回一个已经存在的sparkSession 如果没有会创建
# spark = SparkSession.builder.appName("PYTHON SPARK SQL").config("k1","v1",conf=conf).getOrCreate()
# # 通过sparkSession获取一个sparkContext对象
# sc = spark.sparkContext
# sqlContext = SQLContext(sc)
# # 调用jdbc来读取数据库
# # 这里的坑是需要自己去下载一个oracle的链接驱动来连接oracle 不然会报 no suitable driver的错误（下载的链接驱动放入 spark_home/jars）
# studentDF = spark.read\
#     .format("jdbc")\
#     .option("url","jdbc:oracle:thin:@175.102.18.53:1521:orcl")\
#     .option("dbtable","STUDENT")\
#     .option("user","scott")\
#     .option("password","tiger")\
#     .load()
# scoreDF = spark.read\
#     .format("jdbc") \
#     .option("url","jdbc:oracle:thin:@175.102.18.53:1521:orcl") \
#     .option("dbtable","SCORE") \
#     .option("user","scott") \
#     .option("password","tiger") \
#     .load()

# studentDF.createTempView( "student" )
scoreDF.registerTempTable( "score" )

res = spark.sql("SELECT * FROM score")

# res = studentDF.select("*").where("SNO='101'")
res.show( )
sc.stop( )
