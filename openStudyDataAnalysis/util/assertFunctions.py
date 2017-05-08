# -*- coding: utf-8 -*-
import datetime

OneDayDelta = datetime.timedelta(days=1)

def getOnDayBoolean(r, location=datetime.datetime.now().date()):
    """
    过滤所需统计的数据
    :param r:
    :param location:
    :return:
    """
    rDate = r['recordDate']
    rDate += datetime.timedelta( hours=16 )
    # 然后就是比较
    if isinstance(location,datetime.datetime):
        location = location.date()
    if rDate == location:
        return True
    else:
        return False


def getIsNewBoolean(r, location=datetime.datetime.now( ).date( )):
    """
    判断课程是否是新课程
    :param r:
    :param location:
    :return:
    """
    pDate = (datetime.datetime( r['publishTime'].year, r['publishTime'].month, r['publishTime'].day, 0, 0,
                                0 ) + datetime.timedelta( days=60 )).date( )
    if pDate > location or pDate == location:
        return True
    else:
        return False

def checkIsOnDay(x, T):
    """
    判断是否是前后相差一天的数据
    :param x:
    :param T:
    :return:
    """
    dayData = x[1]
    preXRecordDate = datetime.datetime( dayData[0][0].year, dayData[0][0].month, dayData[0][0].day, 0, 0, 0 )
    endXRecordDate = dayData[1][0]
    if (preXRecordDate + datetime.timedelta( days=T )).date( ) == endXRecordDate:
        return True
    else:
        return False