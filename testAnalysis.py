import sqlite3
import pandas as pd
import numpy as np
from datetime import  datetime, date, time, timedelta
import time
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle


#establish coonection to stockdatabase.sqlite
stockdb=sqlite3.connect('D:\zero\stockdb.sqlite')
cur=stockdb.cursor()

# define related date
td=date.today()
td_str=td.strftime('%Y%m%d')
delta_3w=timedelta(days=21)
delta_1y=timedelta(days=365)
date_3wAgo=td-delta_3w
date_6wAgo=td-delta_3w-delta_3w
date_1yAgo=td-delta_1y


cur.execute("SELECT 股票代码 FROM stocklist")
stocklist=cur.fetchall()

# loop through the stocklist to calculate mean percentage change and standard deviation of percentage change for each stock
for i in stocklist:
    
    stockcode=i[0]
    # skip the stock that has less than 100 rows of data
    cur.execute("SELECT COUNT(changeRatio)  FROM stock_%s" % stockcode)
    records=cur.fetchone()
    if records[0] < 200 : continue
    
    # calculate mean percentage change and standard deviation of percentage change in the period start form a year ago
    cur.execute("SELECT changeRatio FROM stock_%s WHERE date(time) BETWEEN ? and ?" % stockcode,(date_1yAgo, td))
    lst1=cur.fetchall()
    arr1=np.array(lst1)
    meanIn1y=arr1.mean()
    stdIn1y=arr1.std()

    # calculate mean percentage change and standard deviation of percentage change in the three weeks period start from six weeks ago
    cur.execute("SELECT changeRatio FROM stock_%s WHERE date(time) BETWEEN ? and ?" % stockcode,(date_6wAgo, date_3wAgo))
    lst2=cur.fetchall()
    arr2=np.array(lst2)
    meanIn3w=arr2.mean()
    stdIn3w=arr2.std()
    
    # calculate the actual mean percentage change and standard deviation of percentage change in the three weeks period start from three weeks ago
    cur.execute("SELECT changeRatio FROM stock_%s WHERE date(time) BETWEEN ? and ?" % stockcode,(date_3wAgo, td))
    lst3=cur.fetchall()
    arr3=np.array(lst3)
    actual_meanIn3w=arr3.mean()
    actual_stdIn3w=arr3.std()
    
    # write data analysis results into database
    cur.execute("CREATE TABLE IF NOT EXISTS test_%s (stcode VARCHAR UNIQUE, \
             meanInLastYear FLOAT, stdInLastYear FLOAT, meanIn3wHistorical FLOAT, \
             stdIn3wHistorical FLOAT, meanIn3wActual FLOAT, stdIn3wActual FLOAT)" % td_str)
    query="INSERT INTO test_%s" % td_str + \
             "(stcode, meanInLastYear, stdInLastYear, meanIn3wHistorical, \
             stdIn3wHistorical, meanIn3wActual, stdIn3wActual) \
             VALUES (?, ?, ?, ?, ?, ?, ?)"
    values=(stockcode, round(meanIn1y,3), round(stdIn1y, 3), round(meanIn3w, 3), round(stdIn3w, 3), round(actual_meanIn3w, 3), round(actual_stdIn3w, 3))
    try:
        cur.execute(query, values)
    except:
        print('data for stock %s has been analysed' % stockcode)
    
    
    # use the follow statement to break the loop when conduct test
    # break

stockdb.commit()  


cur.execute("SELECT * FROM test_%s ORDER BY meanIn3wHistorical DESC LIMIT 100" % td_str)
top100=cur.fetchall()
    
doc = SimpleDocTemplate("D:\\zero\\analysis_report\\test_report_%s.pdf" %td_str, pagesize=letter)
    
heads=[('stcode', 'meanInLastYear', 'stdInLastYear', 'meanIn3wHistorical', 'stdIn3wHistorical', 'meanIn3wActual', 'stdIn3wActual')]
top100=heads+top100

elements=[]

t=Table(top100, repeatRows=1)
t.setStyle(TableStyle([('GRID', (0,0), (-1,-1), 1, colors.black), ('BACKGROUND', (0,0), (6,0), colors.lightblue), ('TEXTCOLOR', (6,1), (-1,-1), colors.red), ('TEXTCOLOR', (4,1), (-3,-1), colors.red), ('TEXTCOLOR', (2,1), (-5,-1), colors.red)]))
elements.append(t)

doc.build(elements)
 

# close the cursor
cur.close()

# close the database connection
stockdb.close()
