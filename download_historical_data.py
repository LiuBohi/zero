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

if(thsLogin == 0 or thsLogin == -201):
    print('登陆成功')
else:
    print('登陆失败')

# select stocks listed before 31 Dec 1993
cur.execute('SELECT * FROM stocklist')
rows=cur.fetchall()
count=0
# create a for loop to iterate throug each row
for row in rows:
    # count=count+1
    # if count>5: break
    stockcode=row[0]
    thscodes=row[0]+'.'+row[2]
    startDate=row[4]
    d=time.localtime(time.time())
    endDate=time.strftime('%Y-%m-%d',d)

    try:
        cur.execute("CREATE TABLE IF NOT EXISTS stock_%s \
                     (time VARCHAR UNIQUE, thscode VARCHAR, open MONEY, \
                     close MONEY, change MONEY,changeRatio VARCHAR, \
                     volume BIGINT, amount MONEY, transactionAmount BIGINT, \
                     totalShares FLOAT, totalCapital MONEY)" % stockcode)
    except:
        print('creating table stock_%s error ' % stockcode)
        continue

    try:
        thsDataHistoryQuotes=THS_HistoryQuotes(thscodes, \
                             'open;close;change;changeRatio;volume;amount;\
                             transactionAmount;totalShares;totalCapital', \
                             'Interval:D,CPS:1,baseDate:1900-01-01,Currency:YSHB,\
                             fill:Previous', startDate, endDate)
    except:
        print('downloading data of stock %s from iFinD error' % stockcode)
        continue

        # inserting DataFrames into sql table
    try:
        thsData=THS_Trans2DataFrame(thsDataHistoryQuotes)
        thsData.to_sql('stock_%s' % stockcode, con= stockdb, if_exists = 'append', index = False)
        cur.execute('UPDATE stocklist SET 数据截止日 = ? WHERE 股票代码 = ?', (endDate, stockcode))
    except:
        print('update data of stock %s error' % stockcode)



stockdb.commit()
cur.close()
