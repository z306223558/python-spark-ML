# -*- coding: UTF-8 -*-
from SparkConnectError import SparkConnectionException
from mongoSparkConnection import SparkMonogoLoader
from oracleSparkConnection import SparkOracleLoader


class SparkDBConnection():

    loader = None

    def __init__(self, connectionType, conf):

        if not connectionType:
            raise SparkConnectionException("baseConnection", "指定连接类型")
        if connectionType == 'mongodb':
            self.loader = SparkMonogoLoader(conf)
        if connectionType == "oracle":
            if "user" not in conf:
                raise SparkConnectionException("oracleConnection", "请指定数据库用户")
            if "password" not in conf:
                raise SparkConnectionException("oracleConnection", "请指定数据库用户密码")
            if "table" not in conf:
                raise SparkConnectionException("oracleConnection", "请指定数据库表")
            self.loader = SparkOracleLoader(conf)
