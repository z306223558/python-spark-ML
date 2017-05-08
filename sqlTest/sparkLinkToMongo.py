# -*- coding: UTF-8 -*-
import datetime
import sys
from util.SparkConnectionBase import SparkDBConnection
from models.categoryInfo import CategoryInfo

reload( sys )
sys.setdefaultencoding( 'utf-8' )

# timedelta T-Day
OneDayDelta = datetime.timedelta( days=1 )
MAIN_HOST = '175.102.18.112'

mongodbConf = dict(
    host=MAIN_HOST, database="shangHaiStudy", collection="courseRecord",
    appName="factoryClient", sparkMaster="175.105.18.112",
    sqlScheme={"name": "str", "id": "int",
               "recordDate": "datetime"},
    sparkSC=None
)
#初始化loader
loader = SparkDBConnection( "mongodb", mongodbConf ).loader
ctx = loader.SparkSQLInc
test_collection = loader.dbCache().DateFrame

cate = CategoryInfo(ctx,test_collection)
cate.getCateOnDayNum()


# 获取一天的单个分类的总增加数目
def getCateOnDayNum(analysisField="learnNum", dayStr="now", T=1):
    # 指定获取的是哪一天的,应该是获取前一天的数据，记录的那天是传入的那天，recordDate的时间是表示记录的日期，而非是recordDate的增量而是他前一天的增量
    GET_DAY = datetime.datetime.now( ) - OneDayDelta if dayStr == 'now' else datetime.datetime.strptime( dayStr,
                                                                                                         "%Y-%m-%d" ) - OneDayDelta
    GET_DAY_PRE = datetime.datetime( GET_DAY.year, GET_DAY.month, GET_DAY.day ) - datetime.timedelta( days=T )
    insertDay = datetime.datetime.now( ).strftime( "%Y-%m-%d" ) if dayStr == 'now' else dayStr
    if T == 1:
        insertTable = 'category_per_day_' + analysisField
    elif T == 7:
        insertTable = 'category_per_week_' + analysisField
    else:
        insertTable = 'category_per_month_' + analysisField

    # 获当前获取时间的前一天的前T天
    preDay = test_collection.rdd.filter(
        lambda r: getOnDayBoolean( r, GET_DAY_PRE ) ).map(
        lambda r: (r['mainTag'], int( r[analysisField] )) ).reduceByKey( lambda a, b: a + b )

    # 获取当前获取时间的前一天
    curDay = test_collection.rdd.filter(
        lambda r: getOnDayBoolean( r, GET_DAY ) ).map(
        lambda r: (r['mainTag'], int( r[analysisField] )) ).reduceByKey( lambda a, b: a + b )

    # 联合2个rdd，然后做出差的绝对值，得出增长的数量
    curDay.union( preDay ).reduceByKey( lambda a, b: a - b ).map( lambda x: (x[0], abs( x[1] )) ).sortBy(
        lambda y: (y[1], y[0]),
        False ).foreach(
        lambda x: executeMysqlInsert( x, insertDay, insertTable )
    )


# getCateOnDayNum("2017-04-14", 31)

# 获取课程的增量信息
def getCourseIncNum(analysisField="learnNum", dayStr="now", T=1):
    # 限制下获取的条件
    if T not in [1, 7, 31]:
        T = 1

    # 执行插入mysql操作
    def executeMysqlInsert(x, insertDay, insertTable):
        conn = getMySqlConn( )
        if conn is None:
            pass
        else:
            cursor = conn.cursor( )
            cursor.execute(
                "insert into " + insertTable + " (courseId,number,recordDate,courseName) VALUES ('" + x[
                    0] + "','" + str(
                    x[2] ) + "','" + insertDay + "','" + x[1] +
                "')" )
            cursor.close( )
            conn.commit( )
            conn.close( )

    # 指定获取的是哪一天的
    GET_DAY = datetime.datetime.now( ) - OneDayDelta if dayStr == 'now' else datetime.datetime.strptime( dayStr,
                                                                                                         "%Y-%m-%d" ) - OneDayDelta
    GET_DAY_PRE = datetime.datetime( GET_DAY.year, GET_DAY.month, GET_DAY.day ) - datetime.timedelta( days=T )
    insertDay = datetime.datetime.now( ).strftime( "%Y-%m-%d" ) if dayStr == 'now' else dayStr
    if T == 1:
        insertTable = 'course_per_day_' + analysisField
    elif T == 7:
        insertTable = 'course_per_week_' + analysisField
    else:
        insertTable = 'course_per_month_' + analysisField

    # 获当前获取时间的前一天的前T天
    preDay = test_collection.rdd.filter(
        lambda r: getOnDayBoolean( r, GET_DAY_PRE ) ).map(
        lambda r: ((r['id'], r['name']), int( r[analysisField] )) ).reduceByKey( lambda a, b: a + b )

    # 获取当前获取时间的前一天
    curDay = test_collection.rdd.filter(
        lambda r: getOnDayBoolean( r, GET_DAY ) ).map(
        lambda r: ((r['id'], r['name']), int( r[analysisField] )) ).reduceByKey( lambda a, b: a + b )

    # 联合2个rdd，然后做出差的绝对值，得出增长的数量
    curDay.union( preDay ).reduceByKey( lambda a, b: a - b ).map( lambda x: (x[0][0], x[0][1], abs( x[1] )) ).sortBy(
        lambda y: (y[2], y[1], y[0]),
        False ).foreach(
        lambda x:
        executeMysqlInsert( x, insertDay, insertTable )
    )


#  获取新增课程信息
def getNewCourseIncNum(analysisField="learnNum", dayStr="now", T=1):
    # 限制下获取的条件
    if T not in [1, 7, 31]:
        T = 1

    # 过滤所需统计的数据
    def getNewBoolean(r, location=datetime.datetime.now( ).date( )):
        rDate = r['recordDate']
        # 然后就是比较
        if rDate == location:
            return True
        else:
            return False

    def getIsNewBoolean(r, location=datetime.datetime.now( ).date( )):
        pDate = (datetime.datetime( r['publishTime'].year, r['publishTime'].month, r['publishTime'].day, 0, 0,
                                    0 ) + datetime.timedelta( days=60 )).date( )
        if pDate > location or pDate == location:
            return True
        else:
            return False

    # 执行插入mysql操作
    def executeMysqlInsert(x, insertDay, insertTable):
        conn = getMySqlConn( )
        if conn is None:
            pass
        else:
            cursor = conn.cursor( )
            cursor.execute(
                "insert into " + insertTable + " (courseId,number,recordDate,courseName) VALUES ('" + x[
                    0] + "','" + str(
                    x[2] ) + "','" + insertDay + "','" + x[1] +
                "')" )
            cursor.close( )
            conn.commit( )
            conn.close( )

    # 指定获取的是哪一天的
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
    preDay = test_collection.rdd.filter( lambda r: getIsNewBoolean( r ) ).filter(
        lambda r: getOnDayBoolean( r, GET_DAY_PRE ) ).map(
        lambda r: ((r['id'], r['name']), int( r[analysisField] )) ).reduceByKey( lambda a, b: a + b )

    # 获取当前获取时间的前一天
    curDay = test_collection.rdd.filter(
        lambda r: getOnDayBoolean( r, GET_DAY ) ).map(
        lambda r: ((r['id'], r['name']), int( r[analysisField] )) ).reduceByKey( lambda a, b: a + b )

    # 联合2个rdd，然后做出差的绝对值，得出增长的数量
    curDay.union( preDay ).reduceByKey( lambda a, b: a - b ).map( lambda x: (x[0][0], x[0][1], abs( x[1] )) ).sortBy(
        lambda y: (y[2], y[1], y[0]),
        False ).foreach(
        lambda x:
        executeMysqlInsert( x, insertDay, insertTable )
    )


# 直接分析所有的数据的全部增量
def getAllDaysCateIncNum(analysisField="learnNum", T=1):
    if T == 1:
        insertTable = 'category_per_day_' + analysisField
    elif T == 7:
        insertTable = 'category_per_week_' + analysisField
    else:
        insertTable = 'category_per_month_' + analysisField

    def checkIsOnDay(x, T):
        dayData = x[1]

        preXRecordDate = datetime.datetime( dayData[0][0].year, dayData[0][0].month, dayData[0][0].day, 0, 0, 0 )
        endXRecordDate = dayData[1][0]
        if (preXRecordDate + datetime.timedelta( days=T )).date( ) == endXRecordDate:
            return True
        else:
            return False

    # 这个得到的是每天每个大分类下的所有的总数,由于spark的timesteamp类型默认将数据
    dayRdd = test_collection.rdd.map(
        lambda r: (
            (r['mainTag'], (r['recordDate'] + datetime.timedelta( hours=16 )).date( )),
            int( r[analysisField] )) ).reduceByKey(
        lambda a, b: a + b ).map( lambda x: (x[0][0], (x[0][1], x[1])) )

    # 使用rdd的join,然后过滤是我们所需要的前一天数据和后一天数据的元组：(("道德修养",("2017-04-15",40)),("道德修养"，("2017-04-16",50))
    waitRdd = dayRdd.join( dayRdd ).filter( lambda x: checkIsOnDay( x, T ) ).map(
        lambda y: (y[0], y[1][1][0].strftime( "%Y-%m-%d" ), int( y[1][1][1] - y[1][0][1] ),
                   datetime.datetime( y[1][1][0].year, y[1][1][0].month, y[1][1][0].day, 8, 0, 0 ), int( y[1][1][1] )) )
    dayDF = ctx.createDataFrame( waitRdd, schema=["name", "recordDate", "number", "record_datetime", "total_num"] )
    # #写入数据库
    dayDF.write.jdbc( url="jdbc:mysql://175.102.18.112:3306?user=root&password=new.123&characterEncoding=utf8",
                      table="course_analysis." + insertTable, mode="append" )
    print "执行完成"


# getAllDaysCateIncNum("learnNum", 1)
# getAllDaysCateIncNum("learnNum", 7)
# getAllDaysCateIncNum("learnNum", 31)


# 分析所有课程的增量
def getAllCourseIncNum(analysisField="learnNum", T=1):
    # 限制下获取的条
    if T == 1:
        insertTable = 'course_per_day_' + analysisField
    elif T == 7:
        insertTable = 'course_per_week_' + analysisField
    else:
        insertTable = 'course_per_month_' + analysisField

    def checkIsOnDay(x, T):
        # x ： ((id,name), ((date, number),(date, number)))
        dayData = x[1]
        preXRecordDate = datetime.datetime( dayData[0][0].year, dayData[0][0].month, dayData[0][0].day, 0, 0, 0 )
        endXRecordDate = dayData[1][0]
        if (preXRecordDate + datetime.timedelta( days=T )).date( ) == endXRecordDate:
            return True
        else:
            return False

    # 这个得到的是每天每个大分类下的所有的总数 ((id,name), date, number)
    dayRdd = test_collection.rdd.map(
        lambda r: (
            (r['id'], r['name']),
            ((r['recordDate'] + datetime.timedelta( hours=16 )).date( ), int( r[analysisField] ),
             int( r['learnNum'] ))) )

    # (id,name,record,number,datetime,learnNum)  old ((id,name),((date1,number,learnNum),(date2,number,learnNum)))
    waitRdd = dayRdd.join( dayRdd ).filter( lambda x: checkIsOnDay( x, T ) ).map(
        lambda y: (int( y[0][0] ), y[0][1], y[1][1][0].strftime( "%Y-%m-%d" ), int( abs( y[1][1][1] - y[1][0][1] ) ),
                   datetime.datetime( y[1][1][0].year, y[1][1][0].month, y[1][1][0].day, 8, 0, 0 ), y[1][1][2]) )

    dayDF = ctx.createDataFrame( waitRdd, schema=["courseId", "courseName", "recordDate", "number", "record_datetime",
                                                  "total_num"] )
    dayDF.write.jdbc( url="jdbc:mysql://175.102.18.112:3306?user=root&password=new.123&characterEncoding=utf8",
                      table="course_analysis." + insertTable, mode="append" )

    print "执行完成"


#
# getAllCourseIncNum("learnNum", 1)
# getAllCourseIncNum("learnNum", 7)
# getAllCourseIncNum("learnNum", 31)


# 分析所有课程的增量
def getNewAllCourseIncNum(analysisField="learnNum", T=1):
    # 限制下获取的条
    if T == 1:
        insertTable = 'course_new_per_day_' + analysisField
    elif T == 7:
        insertTable = 'course_new_per_week_' + analysisField
    else:
        insertTable = 'course_new_per_month_' + analysisField

    def checkIsOnDay(x, T):
        # x ： ((id,name), ((date, number),(date, number)))
        dayData = x[1]
        preXRecordDate = datetime.datetime( dayData[0][0].year, dayData[0][0].month, dayData[0][0].day, 0, 0, 0 )
        endXRecordDate = dayData[1][0]
        if (preXRecordDate + datetime.timedelta( days=T )).date( ) == endXRecordDate:
            return True
        else:
            return False

    # 过滤出60天之内的数据
    def getIsNewBoolean(r, location=datetime.datetime.now( ).date( )):
        pDate = (r['publishTime'] + datetime.timedelta( hours=16 ) + datetime.timedelta( days=60 )).date( )
        if pDate > location or pDate == location:
            return True
        else:
            return False

    # 这个得到的是每天每个大分类下的所有的总数 ((id,name), date, number)
    dayRdd = test_collection.rdd.filter( lambda x: getIsNewBoolean( x ) ).map(
        lambda r: (
            (r['id'], r['name']),
            ((r['recordDate'] + datetime.timedelta( hours=16 )).date( ), int( r[analysisField] ),
             int( r['learnNum'] ))) )

    # (id,name,record,number,datetime,learnNum)  old ((id,name),((date1,number,learnNum),(date2,number,learnNum)))
    waitRdd = dayRdd.join( dayRdd ).filter( lambda x: checkIsOnDay( x, T ) ).map(
        lambda y: (int( y[0][0] ), y[0][1], y[1][1][0].strftime( "%Y-%m-%d" ), int( abs( y[1][1][1] - y[1][0][1] ) ),
                   datetime.datetime( y[1][1][0].year, y[1][1][0].month, y[1][1][0].day, 8, 0, 0 ), y[1][1][2]) )

    dayDF = ctx.createDataFrame( waitRdd, schema=["courseId", "courseName", "recordDate", "number", "record_datetime",
                                                  "total_num"] )
    dayDF.write.jdbc( url="jdbc:mysql://175.102.18.112:3306?user=root&password=new.123&characterEncoding=utf8",
                      table="course_analysis." + insertTable, mode="append" )

    print "执行完成"

# getNewAllCourseIncNum("learnNum", 1)
# getNewAllCourseIncNum("learnNum", 7)
# getNewAllCourseIncNum("learnNum", 31)

# sc.stop()
# exit()




# # 获取分类的历史数据
# getCateOnDayNum("learnNum", "2017-04-17", T=1)

# getCateOnDayNum("learnNum", "2017-04-15", T=7)
# getCateOnDayNum("learnNum", "2017-04-15", T=31)
# # 获取课程的历史数据
# getCourseIncNum("learnNum", "2017-04-15", T=1)
# getCourseIncNum("learnNum", "2017-04-15", T=7)
# getCourseIncNum("learnNum", "2017-04-15", T=31)
# # 获取新加入课程的历史数据
# getNewCourseIncNum("learnNum", "2017-03-11", T=1)
# for i in range(1,40):
#     catchDate = datetime.datetime.strptime("2017-04-16","%Y-%m-%d") - datetime.timedelta(days=i)
#     # print catchDate.strftime("%Y-%m-%d")
#     getNewCourseIncNum("learnNum", catchDate.strftime("%Y-%m-%d"), T=1)
# getNewCourseIncNum("learnNum", "2017-04-16", T=7)
# getNewCourseIncNum("learnNum", "2017-04-16", T=31)
