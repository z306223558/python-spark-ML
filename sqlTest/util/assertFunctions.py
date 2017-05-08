# -*- coding: utf-8 -*-
import datetime

# 过滤所需统计的数据
def getOnDayBoolean(r, location=datetime.datetime.now( )):
    rDate = r['recordDate']
    rDate += datetime.timedelta( hours=16 )
    # 然后就是比较
    if rDate.date( ) == location.date( ):
        return True
    else:
        return False