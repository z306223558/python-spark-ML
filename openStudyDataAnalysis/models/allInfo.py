# -*- coding: utf-8 -*-
# 获取一天的单个分类的总增加数目
import datetime
import sys
sys.path.append("../")
from util.assertFunctions import  getOnDayBoolean

class CategoryInfo():

    def __init__(self,ctx,df):
        self.ctx = ctx
        self.df = df


    def getAllDaysCateIncNum(self,analysisField="learnNum", T=1):
        """
        一次性获取所有分类数据的信息，分日 周 月
        :param analysisField:
        :param T:
        :return:
        """
        if T == 1:
            insertTable = 'category_per_day_' + analysisField
        elif T == 7:
            insertTable = 'category_per_week_' + analysisField
        else:
            insertTable = 'category_per_month_' + analysisField

        # 这个得到的是每天每个大分类下的所有的总数,由于spark的timesteamp类型默认将数据
        dayRdd = self.df.rdd.map(
            lambda r: (
                (r['mainTag'], (r['recordDate'] + datetime.timedelta( hours=16 )).date( )),
                int( r[analysisField] )) ).reduceByKey(
            lambda a, b: a + b ).map( lambda x: (x[0][0], (x[0][1], x[1])) )

        # 使用rdd的join,然后过滤是我们所需要的前一天数据和后一天数据的元组：(("道德修养",("2017-04-15",40)),("道德修养"，("2017-04-16",50))
        waitRdd = dayRdd.join( dayRdd ).filter( lambda x: checkIsOnDay( x, T ) ).map(
            lambda y: (y[0], y[1][1][0].strftime( "%Y-%m-%d" ), int( y[1][1][1] - y[1][0][1] ),
                       datetime.datetime( y[1][1][0].year, y[1][1][0].month, y[1][1][0].day, 8, 0, 0 ), int( y[1][1][1] )) )
        try:
            dayDF = self.ctx.createDataFrame( waitRdd, schema=["name", "recordDate", "number", "record_datetime", "total_num"] )
        except Exception as e:
            print "获取数据的rdd为空，记录失败，执行失败"
            return
        # #写入数据库
        dayDF.write.jdbc( url="jdbc:mysql://175.102.18.112:3306?user=root&password=new.123&characterEncoding=utf8",
                          table="course_analysis." + insertTable, mode="append" )
        print "获取所有分类信息操作执行完成"

    def getAllCourseIncNum(self,analysisField="learnNum", T=1):
        """
        一次性获取所有课程数据的信息，分日 周 月
        :param analysisField:
        :param T:
        :return:
        """
        # 限制下获取的条
        if T == 1:
            insertTable = 'course_per_day_' + analysisField
        elif T == 7:
            insertTable = 'course_per_week_' + analysisField
        else:
            insertTable = 'course_per_month_' + analysisField

            # 这个得到的是每天每个大分类下的所有的总数 ((id,name), date, number)
        dayRdd = self.df.rdd.map(
            lambda r: (
                (r['id'], r['name']),
                ((r['recordDate'] + datetime.timedelta( hours=16 )).date( ), int( r[analysisField] ),
                 int( r['learnNum'] ))) )

        # (id,name,record,number,datetime,learnNum)  old ((id,name),((date1,number,learnNum),(date2,number,learnNum)))
        waitRdd = dayRdd.join( dayRdd ).filter( lambda x: checkIsOnDay( x, T ) ).map(
            lambda y: (int( y[0][0] ), y[0][1], y[1][1][0].strftime( "%Y-%m-%d" ), int( abs( y[1][1][1] - y[1][0][1] ) ),
                       datetime.datetime( y[1][1][0].year, y[1][1][0].month, y[1][1][0].day, 8, 0, 0 ), y[1][1][2]) )


        try:
            dayDF = self.ctx.createDataFrame( waitRdd, schema=["courseId", "courseName", "recordDate", "number", "record_datetime",
                                                      "total_num"] )
        except Exception as e:
            print "获取数据的rdd为空，记录失败，执行失败"
            return
        dayDF.write.jdbc( url="jdbc:mysql://175.102.18.112:3306?user=root&password=new.123&characterEncoding=utf8",
                          table="course_analysis." + insertTable, mode="append" )
        print "获取所有课程信息操作执行完成"


    def getNewAllCourseIncNum(self,analysisField="learnNum", T=1):
        # 限制下获取的条
        if T == 1:
            insertTable = 'course_new_per_day_' + analysisField
        elif T == 7:
            insertTable = 'course_new_per_week_' + analysisField
        else:
            insertTable = 'course_new_per_month_' + analysisField



        # 这个得到的是每天每个大分类下的所有的总数 ((id,name), date, number)
        dayRdd = self.df.rdd.filter( lambda x: getIsNewBoolean( x ) ).map(
            lambda r: (
                (r['id'], r['name']),
                ((r['recordDate'] + datetime.timedelta( hours=16 )).date( ), int( r[analysisField] ),
                 int( r['learnNum'] ))) )

        # (id,name,record,number,datetime,learnNum)  old ((id,name),((date1,number,learnNum),(date2,number,learnNum)))
        waitRdd = dayRdd.join( dayRdd ).filter( lambda x: checkIsOnDay( x, T ) ).map(
            lambda y: (int( y[0][0] ), y[0][1], y[1][1][0].strftime( "%Y-%m-%d" ), int( abs( y[1][1][1] - y[1][0][1] ) ),
                       datetime.datetime( y[1][1][0].year, y[1][1][0].month, y[1][1][0].day, 8, 0, 0 ), y[1][1][2]) )

        try:
            dayDF = self.ctx.createDataFrame( waitRdd, schema=["courseId", "courseName", "recordDate", "number", "record_datetime",
                                                      "total_num"] )
        except Exception as e:
            print "获取数据的rdd为空，记录失败，执行失败"
            return

        dayDF.write.jdbc( url="jdbc:mysql://175.102.18.112:3306?user=root&password=new.123&characterEncoding=utf8",
                          table="course_analysis." + insertTable, mode="append" )

        print "获取所有新课程数据操作执行完成"