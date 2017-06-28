# -*- coding: UTF-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.types import *


class SparkMongodbLoader():
    spark = None
    SparkConfInc = None
    SparkSQLInc = None
    SQLScheme = None
    MongoConn = None
    DataFrame = None

    def __init__(self, conf):
        self.conf = conf


    def getSparkSession(self):
        """获取sparkSession的实例"""
        self.spark = SparkSession\
            .builder\
            .master("local[*]")\
            .appName(self.conf["appName"])\
            .config("spark.sql.warehouse.dir",self.conf["sparkWarehousePath"])\
            .config("spark.sql.shuffle.partitions",6)\
            .config("spark.work.memory","4g")\
            .getOrCreate()
        return self.spark


    def getMongodbScheme(self):
        """返回mongodb的load指定的字段"""
        if self.conf["inputSqlScheme"]:
            schemeInfo = self.conf["inputSqlScheme"]
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
        """获取mongodb连接实例"""
        if not self.MongoConn:
            if not self.spark:
                self.getSparkSession()
                self.getMongodbScheme()
                if self.SQLScheme:
                    self.MongoConn = self.spark.read.schema(self.SQLScheme).format(
                        "com.mongodb.spark.sql").options(
                        uri=self.conf["inputUri"],
                        database=self.conf["inputBase"],
                        collection=self.conf["inputCollection"]
                    )
                else:
                    self.MongoConn = self.spark.read.format("com.mongodb.spark.sql").options(
                        uri=self.conf["inputUri"],
                        database=self.conf["inputBase"],
                        collection=self.conf["inputCollection"]
                    )

    def dbLoad(self):
        """执行数据导入"""
        if not self.MongoConn:
            self.getMongodbInc()
        self.DataFrame = self.MongoConn.load()
        return self

    def dbCache(self):
        """执行数据导入并cache对应的DF"""
        if not self.MongoConn:
            self.getMongodbInc()
        self.DataFrame = self.MongoConn.load().cache()
        return self
