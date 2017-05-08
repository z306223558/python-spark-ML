# -*- coding: utf-8 -*-
# SparkConnectionException to a few of types of databases

class SparkConnectionException(Exception):

    """自定义的spark连接各种数据库的异常抛出"""

    def __init__(self, connectType, errorStr):
        Exception.__init__(self)
        self.connectType = connectType
        self.errorStr = errorStr