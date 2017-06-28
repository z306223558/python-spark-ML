# -*- coding: UTF-8 -*-
from SparkConnectError import SparkConnectionException
from mongoSparkConnection import SparkMongodbLoader
from oracleSparkConnection import SparkOracleLoader


class SparkDBConnection():

    loader = None

    def __init__(self, conf):

        if not conf['inputType']:
            raise SparkConnectionException("baseConnection", "指定连接类型")
        if conf['inputType'] == 'mongodb':
            self.loader = SparkMongodbLoader(conf)
        if conf['inputType'] == "oracle":
            if "user" not in conf:
                raise SparkConnectionException("oracleConnection", "请指定数据库用户")
            if "password" not in conf:
                raise SparkConnectionException("oracleConnection", "请指定数据库用户密码")
            if "table" not in conf:
                raise SparkConnectionException("oracleConnection", "请指定数据库表")
            self.loader = SparkOracleLoader(conf)
