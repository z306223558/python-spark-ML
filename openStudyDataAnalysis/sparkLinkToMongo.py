# -*- coding: UTF-8 -*-
import sys

reload( sys )
sys.setdefaultencoding( 'utf-8' )

from util.SparkConnectionBase import SparkDBConnection
from models.categoryInfo import CategoryInfo
from models.tf_idf import TF_IDFTest
from models.lda import LDATest
if __name__ == "__main__":
    MAIN_HOST = '127.0.0.1'
    SPARK_HOST = 'local[*]'
    mongodbConf = dict(
        host=MAIN_HOST, database="shangHaiStudy", collection="courseRecord",
        appName="factoryClient", sparkMaster="175.105.18.112",
        sqlScheme={"name": "str", "id": "int",
                   "recordDate": "datetime"},
        sparkSC=None
    )
    mongodbConf = dict(
        host=MAIN_HOST, database="openCourse", collection="college_html",
        appName="ldaTest", sparkMaster="175.105.18.112",
        sqlScheme={"html": "str","title": "str"},
        sparkSC=None
    )

    loader = SparkDBConnection( "mongodb", mongodbConf ).loader
    test_collection = loader.dbCache().DateFrame
    ctx = loader.SparkSQLInc
    sc = loader.SparkInc
    lda = LDATest(ctx,test_collection,sc=sc)
    lda.clustering()
    sc.stop()

