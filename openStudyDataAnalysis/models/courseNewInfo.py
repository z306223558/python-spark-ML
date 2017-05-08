# -*- coding: utf-8 -*-
# 获取一天的单个分类的总增加数目
import datetime
import sys
sys.path.append("../")
from util.assertFunctions import  *

class CourseInfo():

    def __init__(self,ctx,df):
        self.ctx = ctx
        self.df = df

    def getCourseIncNum(self,analysisField="learnNum", dayStr="now", T=1):
        """
        获取新增课程的增量信息
        :param analysisField:
        :param dayStr:
        :param T:
        :return:
        """

        GET_DAY = datetime.datetime.now( ) - OneDayDelta if dayStr == 'now' else datetime.datetime.strptime( dayStr,
                                                                                                             "%Y-%m-%d" )
        GET_DAY_PRE = datetime.datetime( GET_DAY.year, GET_DAY.month, GET_DAY.day ) - datetime.timedelta( days=T )
        insertDay = datetime.datetime.now( ).strftime( "%Y-%m-%d" ) if dayStr == 'now' else dayStr
        if T == 1:
            insertTable = 'course_new_per_day_' + analysisField
        elif T == 7:
            insertTable = 'course_new_per_week_' + analysisField
        else:
            insertTable = 'course_new_per_month_' + analysisField

        # 获当前获取时间的前一天的前T天
        preDay = self.df.rdd.filter( lambda r: getIsNewBoolean( r ) ).filter(
            lambda r: getOnDayBoolean( r, GET_DAY_PRE ) ).map(
            lambda r: ((r['id'], r['name']), int( r[analysisField] )) ).reduceByKey( lambda a, b: a + b )

        # 获取当前获取时间的前一天
        curDay = self.df.rdd.filter(
            lambda r: getOnDayBoolean( r, GET_DAY ) ).map(
            lambda r: ((r['id'], r['name']), int( r[analysisField] )) ).reduceByKey( lambda a, b: a + b )

        # 联合2个rdd，然后做出差的绝对值，得出增长的数量
        waitRdd = curDay.union( preDay ).reduceByKey( lambda a, b: a - b ).map( lambda x: (x[0][0], x[0][1], abs( x[1] )) )
        try:
            waitDF = self.ctx.createDataFrame(waitRdd,schema=["name", "recordDate"])
        except Exception as e:
            print "数据为空，今天的数据统计无"
            return

        waitDF.write.jdbc( url="jdbc:mysql://175.102.18.112:3306?user=root&password=new.123&characterEncoding=utf8",
                           table="course_analysis." + insertTable, mode="append" )

