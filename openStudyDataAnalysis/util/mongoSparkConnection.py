# -*- coding: UTF-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.types import *


class SparkMonogoLoader():
    SparkInc = None
    SparkConfInc = None
    SparkSQLInc = None
    SQLScheme = None
    MongoConn = None
    DateFrame = None

    def __init__(self, conf):

        # sparkSC
        if "sparkSC" in conf:
            self.SparkInc = conf['sparkSC']
        # 设置运行环境
        self.isLocal = conf['isLocal'] if "isLocal" in conf else True
        # mongo基本的配置
        self.host = conf["host"] if "host" in conf else "127.0.0.1"
        self.port = conf["port"] if "port" in conf else 27017
        self.database = conf["database"] if "database" in conf else "test"
        self.collection = conf["collection"] if "collection" in conf else "test"

        # mongo scheme
        self.SQLScheme = conf["sqlScheme"] if "sqlScheme" in conf else None

        # sparkConf的基本配置
        self.sparkAppName = conf['appName'] if "appName" in conf else ""
        self.sparkMaster = conf['sparkMaster'] if "sparkMaster" in conf else "local[*]"
        self.sparkMasterPort = conf['sparkMasterPort'] if "sparkMasterPort" in conf else 7077
        self.sparkOtherConf = conf['otherConf'] if "otherConf" in conf else {}
        self.sparkMasterUrl = "spark://" + self.sparkMaster + ":" + str(self.sparkMasterPort)


    def getSparkSession(self):
        spark_warehouse_path = "file:///home/xiaojun/sparkmongo/spark-warehouse"
        if self.isLocal:
            spark_warehouse_path = "/Users/xiaojun/pythonDir/sparkmongo/spark-warehouse"
        self.SparkInc = SparkSession\
            .builder\
            .master(self.sparkMasterUrl)\
            .appName(self.sparkAppName)\
            .config("spark.sql.warehouse.dir",spark_warehouse_path)\
            .enableHiveSupport()\
            .getOrCreate()
        self.SparkInc.conf.set("spark.sql.shuffle.partitions",6)
        self.SparkInc.conf.set("spark.executor.memory","4g")

    def getMongoScheme(self):
        if self.SQLScheme:
            schemeInfo = self.SQLScheme
            self.SQLScheme = []
            if isinstance(schemeInfo, dict):
                # 格式化出scheme
                for k in schemeInfo:
                    if schemeInfo[k] == "str":
                        self.SQLScheme.append(StructField(k, StringType(), True))
                    elif schemeInfo[k] == "int":
                        self.SQLScheme.append(StructField(k, IntegerType(), True))
                    elif schemeInfo[k] == "datetime":
                        self.SQLScheme.append(StructField(k, DateType(), True))
                    elif schemeInfo[k] == "timestamp":
                        self.SQLScheme.append(StructField(k, TimestampType(), True))
                    elif schemeInfo[k] == "array":
                        self.SQLScheme.append(StructField(k, ArrayType, True))
                    else:
                        self.SQLScheme.append(StructField(k, StringType(), True))
            if isinstance(schemeInfo, list):
                for v in schemeInfo:
                    self.SQLScheme.append(StructField(v, StringType(), True))
            if isinstance(schemeInfo, str):
                for s in str(schemeInfo).split(" "):
                    self.SQLScheme.append(StructField(s, StringType(), True))
            if self.SQLScheme:
                self.SQLScheme = StructType(self.SQLScheme)

    def getMongodbInc(self):
        if not self.MongoConn:
            if not self.SparkInc:
                self.getSparkSession()
                # 执行获取scheme
                self.getMongoScheme()
                # 判断scheme是否存在
                if self.SQLScheme:
                    self.MongoConn = self.SparkInc.read.schema(self.SQLScheme).format(
                        "com.mongodb.spark.sql").options(
                        uri="mongodb://" + self.host + ":" + str(self.port),
                        database=self.database,
                        collection=self.collection
                    )
                else:
                    self.MongoConn = self.SparkInc.read.format("com.mongodb.spark.sql").options(
                        uri="mongodb://" + self.host + ":" + str(self.port),
                        database=self.database,
                        collection=self.collection
                    )

    def dbLoad(self):
        if not self.MongoConn:
            self.getMongodbInc()
        self.DateFrame = self.MongoConn.load()
        return self

    def dbCache(self):
        if not self.MongoConn:
            self.getMongodbInc()
        self.DateFrame = self.MongoConn.load().cache()
        return self
