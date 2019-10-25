import xlrd
import sqlite3

# establish database connection
stockdb=sqlite3.connect('D:\zero\stockdb.sqlite')
cur=stockdb.cursor()

# open the workbook and define the worksheet
while True:
    print('Please enter the stock market code: sz or sh')
    stockmarket=input()
    try:
        book=xlrd.open_workbook('D:\zero\stock_list_%s.xlsx' % stockmarket)
        sheet=book.sheet_by_name('a_share_list')
        market=stockmarket.upper()
    except:
        print('The stock market code is not correct')
        exit()

    # creat stocklist table if not exists
    cur.execute('CREATE TABLE IF NOT EXISTS stocklist (股票代码 VARCHAR UNIQUE, \
                 上市日期 VARCHAR, 上市地点 VARCHAR, 数据起始日 VARCHAR, \
                 数据截止日 VARCHAR)')

    # creat the INSERT INTO sql query
    query='INSERT INTO stocklist \
           (股票代码, 上市日期, 上市地点, 数据起始日, 数据截止日) \
           VALUES (?, ?, ?, ?, ?)'

    # creat a for loop to iterate through each row in the XLS file, starting at row 2 skip the headers
    for r in range(1, sheet.nrows):
        股票代码=sheet.cell_value(r, 0)
        上市日期=sheet.cell_value(r,1)

        # assign values from each row
        values=(股票代码, 上市日期, market, '2015-01-01', '2015-01-01')

        # execute sql query
        cur.execute(query,values)

    # commit the transaction
    stockdb.commit()
    print('stockes listed on %s market are added to the list' % stockmarket)

# close the cursor
cur.close()

# close the database connection
stockdb.close()
