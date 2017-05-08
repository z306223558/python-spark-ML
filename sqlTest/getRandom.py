# -*- coding: UTF-8 -*-
import math
import random
from datetime import datetime

def getNum(my_time,num):
    caseNum = int(random.random() * 4)
    day = (datetime.now() - my_time).days
    if caseNum == 0:
        return get_norm(day,num)
    elif caseNum == 1:
        return get_num(day,num)
    elif caseNum == 2:
        return get_sin(day,num)
    elif caseNum == 3:
        return get_norm(day,num)

def get_num(day,num):
    arr = []
    for i in range(0, 366):
        if(i ==0):
            arr.append(int(num))
        else:
            if(int(random.random()*150) ==0):
                arr.append(num)
            else:
                num = num + (i-188)/365 + (random.random()*num/60+random.random()*num/52)/12
                arr.append()
    return arr


def get_sin(day,num):
    calc = 180.0/365
    arr = []
    for i in range(0, 366):
        if(i ==0):
            arr.append(int(num))
        else:
            if(int(random.random()*150) ==0):
                arr.append(num)
            else:
                num = num + random.random()*num/48*math.sin(math.radians(abs(i-188)*calc))
                arr.append(num)
    return arr


def get_norm(day,num):
    a = 365/2 + num*(day/365.0)
    sigma = 4000 + 5000*random.random()
    arr = []
    b = 365/2+num*(day/365.0)
    for i in range(0, 366):
        if(i ==0):
            arr.append(int(num))
        else:
            if(int(random.random()*150) ==0):
                arr.append(num)
            else:
                num = num+(num/36)*random.random() *norm(a,sigma,b)
                arr.append(int(num))
    return arr




def st_norm(u):
    x=abs(u)/math.sqrt(2)
    T=(0.0705230784,0.0422820123,0.0092705272,
       0.0001520143,0.0002765672,0.0000430638)
    E=1-pow((1+sum([a*pow(x,(i+1))
                    for i,a in enumerate(T)])),-16)
    p=0.5-0.5*E if u<0 else 0.5+0.5*E
    return(p)


def norm(a,sigma,x):
    u=(x-a)/sigma
    return(st_norm(u))