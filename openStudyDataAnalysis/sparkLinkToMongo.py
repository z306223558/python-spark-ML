# -*- coding: UTF-8 -*-
import datetime
import sys

reload( sys )
sys.setdefaultencoding( 'utf-8' )

from util.SparkConnectionBase import SparkDBConnection
from models.categoryInfo import CategoryInfo

if __name__ == "__main__":
    MAIN_HOST = '175.102.18.112'
    mongodbConf = dict(
        host=MAIN_HOST, database="shangHaiStudy", collection="courseRecord",
        appName="factoryClient", sparkMaster="175.105.18.112",
        sqlScheme={"name": "str", "id": "int",
                   "recordDate": "datetime"},
        sparkSC=None
    )

    loader = SparkDBConnection( "mongodb", mongodbConf ).loader
    test_collection = loader.dbCache().DateFrame
    ctx = loader.SparkSQLInc
    cate = CategoryInfo(ctx,test_collection)
    cate.getCateOnDayNum()

