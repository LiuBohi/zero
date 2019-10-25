# -*- coding: utf-8 -*-
"""
Created on Fri Oct 18 18:48:07 2019

@author: liubo
"""

import sqlite3
from iFinDPy import *
import json
import pandas as pd
import time
import datetime

# establish coonection to stockdatabase.sqlite
stockdb=sqlite3.connect('D:\zero\stockdb.sqlite')
cur=stockdb.cursor()

# log in iFinD using user name and key
username=input('Please enter iFinD username: ')
passcode=input('Please enter iFinD password: ')
thsLogin = THS_iFinDLogin(username, passcode)
try:
    if (thsLogin == 0 or thsLogin == -201):
        print('登陆成功')
except:
    print('登陆失败')
    exit()

# select stocks from stocklist
cur.execute('SELECT * FROM stocklist')
rows=cur.fetchall()

# create a for loop to iterate through each row
count=0
for row in rows:
    
    # use the below script to break the loop in test
    # count=count+1
    # if count > 1: break
    stockcode=row[0]
    thscodes=row[0]+'.'+row[2]
    startDate=row[4]
    d=time.localtime(time.time())
    endDate=time.strftime('%Y-%m-%d',d)

    # download data from iFinD
    try:
        thsDataHistoryQuotes=THS_HistoryQuotes('600000.SH','open;close;change;changeRatio;volume;amount;transactionAmount;totalShares;totalCapital','Interval:D,CPS:1,baseDate:1900-01-01,Currency:YSHB,fill:Previous',startDate,endDate)
    except:
        print('downloading data error: %s ' % stockcode)
        continue

    # transform JSON results to DataFreame
    thsData=THS_Trans2DataFrame(thsDataHistoryQuotes)
    thsData.head()
    
    # inserting DataFrames into SQL table
    for index, series in thsData.iterrows():
        query='INSERT INTO stock_%s (time, thscode, open, close, change, changeRatio, volume, amount, transactionAmount, totalShares, totalCapital) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)' % stockcode
        print(series)
        values=(series['time'], series['thscode'], series['open'], series['close'], series['change'], series['changeRatio'], series['volume'], series['amount'], series['transactionAmount'], series['totalShares'], series['totalCapital'])
        try:
            cur.execute(query, values)
            cur.execute("SELECT * FROM stock_%s ORDER BY time DESC" % stockcode)
            r=cur.fetchone()
            lastDate=r[0]
            cur.execute('UPDATE stocklist SET 数据截止日 = ? WHERE 股票代码 = ?', (lastDate, stockcode))
        except:
            print('insert data error: %s ' % stockcode)
            continue


stockdb.commit()

cur.close()
stockdb.close()
