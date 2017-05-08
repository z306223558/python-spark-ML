# -*- coding: utf-8 -*-
# 获取一天的单个分类的总增加数目
import datetime
import sys
sys.path.append("../")
from util.assertFunctions import  getOnDayBoolean

class CategoryInfo():

    def __init__(self,ctx,df):
        self.oneDayDelta = datetime.timedelta(days=1)
        print ctx
        self.ctx = ctx
        self.df = df

    def getCateOnDayNum(self,analysisField="learnNum", dayStr="now", T=1):
        """
        指定获取的是哪一天的,应该是获取前一天的数据，记录的那天是传入的那天，recordDate的时间是表示记录的日期，而非是recordDate的增量而是他前一天的增量
        :param analysisField:
        :param dayStr:
        :param T:
        :return:
        """

        GET_DAY = datetime.datetime.now( ) - self.oneDayDelta if dayStr == 'now' else datetime.datetime.strptime( dayStr,
                                                                                                             "%Y-%m-%d" ) - self.oneDayDelta
        GET_DAY_PRE = datetime.datetime( GET_DAY.year, GET_DAY.month, GET_DAY.day ) - datetime.timedelta( days=T )

        if T == 1:
            insertTable = 'category_per_day_' + analysisField
        elif T == 7:
            insertTable = 'category_per_week_' + analysisField
        else:
            insertTable = 'category_per_month_' + analysisField

        # 获当前获取时间的前一天的前T天
        preDay = self.df.rdd.filter(
            lambda r: getOnDayBoolean( r, GET_DAY_PRE ) ).map(
            lambda r: (r['mainTag'], int( r[analysisField] )) ).reduceByKey( lambda a, b: a + b )

        # 获取当前获取时间的前一天
        curDay = self.df.rdd.filter(
            lambda r: getOnDayBoolean( r, GET_DAY ) ).map(
            lambda r: (r['mainTag'], int( r[analysisField] )) ).reduceByKey( lambda a, b: a + b )

        # 联合2个rdd，然后做出差的绝对值，得出增长的数量
        waitRdd = curDay.union( preDay ).reduceByKey( lambda a, b: a - b ).map( lambda x: (x[0], abs( x[1] )))

        try:
            waitDF = self.ctx.createDataFrame(waitRdd,schema=["name", "recordDate"])
        except Exception as e:
            print "数据为空，今天的数据统计无"
            return

        waitDF.write.jdbc( url="jdbc:mysql://175.102.18.112:3306?user=root&password=new.123&characterEncoding=utf8",
                          table="course_analysis." + insertTable, mode="append" )