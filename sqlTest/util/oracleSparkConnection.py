# -*- coding: UTF-8 -*-
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext


class SparkOracleLoader( ):
    SparkInc = None
    SparkConfInc = None
    SparkSQLInc = None
    SQLScheme = None
    OracleConn = None
    DateFrame = None

    def __init__(self, conf):

        # sparkSC
        if "sparkSC" in conf:
            self.SparkInc = conf['sparkSC']

        # oracle基本的配置
        self.host = conf["host"] if "host" in conf else "127.0.0.1"
        self.port = conf["port"] if "port" in conf else 27017
        self.collection = conf["table"] if "collection" in conf else "test"
        # 必须的参数判断
        self.collection = conf["table"] if "collection" in conf else "test"
        self.collection = conf["table"] if "collection" in conf else "test"
        self.oracleType = conf["connectionType"] if "connectionType" in conf else "thin"
        self.oracleConnectionKey = conf["oracleConnectionKey"] if "oracleConnectionKey" in conf else "orcl"
        self.dbUser = conf["user"]
        self.password = conf['password']
        self.table = conf['table']

        # sparkConf的基本配置
        self.sparkAppName = conf['appName'] if "appName" in conf else ""
        self.sparkMaster = conf['sparkMaster'] if "sparkMaster" in conf else "local[*]"
        self.sparkMasterPort = conf['sparkMasterPort'] if "sparkMasterPort" in conf else 7077
        self.sparkOtherConf = conf['otherConf'] if "othenrConf" in conf else {}

        self.sparkMasterUrl = "spark://" + self.sparkMaster + ":" + str( self.sparkMasterPort )

    def setConf(self):
        self.sparkConfInc = SparkConf( ).setAppName( self.sparkAppName ).setMaster( self.sparkMasterUrl )
        for k in self.sparkOtherConf:
            self.sparkConfInc.set( k, self.sparkOtherConf[k] )

    def getSparkInc(self):
        if not self.SparkConfInc:
            self.setConf( )
            self.SparkInc = SparkContext( self.sparkOtherConf )


    def getSparkSQLInc(self):
        if not self.SparkInc:
            self.getSparkInc( )
            self.SparkSQLInc = SQLContext( self.SparkInc )
        else:
            self.SparkSQLInc = SQLContext(self.SparkInc)


    def getOracleInc(self):
        if not self.OracleConn:
            if not self.SparkSQLInc:
                self.getSparkSQLInc()
                self.OracleConn = self.SparkSQLInc.read \
                    .format( "jdbc" ) \
                    .option( "url", "jdbc:oracle:" + self.oracleType + ":@" + self.host + ":" + str(
                    self.port ) + ":" + self.oracleConnectionKey ) \
                    .option( "dbtable", self.table ) \
                    .option( "user", self.dbUser ) \
                    .option( "password", self.password )

    def dbLoad(self):
        if not self.OracleConn:
            self.getOracleInc( )
        self.DateFrame = self.OracleConn.load( )
        return self

    def dbCache(self):
        if not self.OracleConn:
            self.getOracleInc( )
        self.DateFrame = self.OracleConn.load( ).cache( )
        return self
