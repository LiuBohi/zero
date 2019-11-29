import sqlite3
import numpy as np
import pandas as pd
from datetime import datetime,timedelta

# construct CSI300 stock list
stockdb=sqlite3.connect("D:/zero/stock.sqlite")
stockcur=stockdb.cursor()
stockcur.execute("SELECT 股票代码 FROM stocklist WHERE CSI300 = 1")
csilst = stockcur.fetchall()
# load close price data for CSI300 stocks
dat = pd.DataFrame()
for s in csilst:
    stockcode = s[0]
    df_temp = pd.read_sql("SELECT time,close as [%s] FROM stock_%s" % (stockcode,stockcode),stockdb,\
                     index_col = 'time', parse_dates={'time':'%Y-%m-%d'})
    dat = pd.concat([dat,df_temp], axis = 1,)
# calculate standard deviation for each stock
stds = np.std(dat)
# load test parameters for backtest and run the test
testPara = pd.read_csv('D:/zero/backtest/test_parameters.csv',index_col = 'numTest')
for numTest in testPara.index:
    d_back = timedelta(days=int(testPara.loc[numTest,'window']))     # define the moving window for the rolling average
    cash = testPara.loc[numTest,'iCapital']                   # initial invested capital
    hp = timedelta(days=int(testPara.loc[numTest,'holdingPeriods']))         # hold stock no more than 30 days
    low_score = testPara.loc[numTest,'low_score']                  # when stock price dropped below one standard deviation of the mean of the last days
    high_score = testPara.loc[numTest,'high_score']                # when stock price rised below one standard deviation of the mean of the last days
    inv_prop = testPara.loc[numTest,'invProp']

    # create an empty dataframe to recorde the transactions and postions
    posBook = pd.DataFrame(columns=['stockcode','buy_time','numShares','cost','latest_price','value'])
    posBook = posBook.set_index('stockcode')
    posBook.loc['000000','value'] = cash
    portfolio = np.sum(posBook.value)
    transBook = pd.DataFrame()
    trans_lst=[]

    # loop through the date index and check the transaction conditions
    for d in dat.index:
        if d - d_back < dat.index[0] : continue # skip the first days contained totally by the moving window
        for i in csilst:
            stockcode = i[0]

            if stockcode in posBook.index and np.isnan(posBook.loc[stockcode,'numShares']):
                posBook = posBook.drop(stockcode,axis = 0)
            # check to see when to sell shares held in the position book
            elif stockcode in posBook.index and not np.isnan(posBook.loc[stockcode,'numShares']):
                numShares = posBook.loc[stockcode,'numShares']
                buy_time = posBook.loc[stockcode,'buy_time']

                # sell when holding periods extend out of predefined periods
                if (d-buy_time) > hp:
                    cash = posBook.loc['000000','value']
                    cash = cash + numShares * dat.loc[d,stockcode]
                    posBook.loc['000000','value'] = cash
                    posBook = posBook.drop(stockcode,axis = 0)
                    trans_lst.append([datetime.strftime(d,"%Y-%m-%d"),stockcode,'sell_hp', \
                            np.sum(posBook.value),numShares * dat.loc[d,stockcode],-numShares,dat.loc[d,stockcode]])
                # sell when price arised more than a few standard deviation above the moving average
                elif (  (dat.loc[d,stockcode] - np.mean(dat.loc[d-d_back:d,stockcode])  )/stds[stockcode]) > high_score:
                    cash = posBook.loc['000000','value']
                    cash = cash + numShares * dat.loc[d,stockcode]
                    posBook.loc['000000','value'] = cash
                    posBook = posBook.drop(stockcode,axis = 0)
                    trans_lst.append([datetime.strftime(d,"%Y-%m-%d"),stockcode,'sell_price', \
                            np.sum(posBook.value),numShares * dat.loc[d,stockcode],-numShares,dat.loc[d,stockcode]])
                # update any stock in the postion book to the latest price
                else:
                    posBook.loc[stockcode,['latest_price','value']] = [dat.loc[d,stockcode], numShares * dat.loc[d,stockcode]]

            # check to see whether to buy the stock
            elif stockcode not in posBook.index and ((dat.loc[d,stockcode] - np.mean(dat.loc[d-d_back:d,stockcode]))/stds[stockcode]) > low_score :
                cash = posBook.loc['000000','value']
                # force the buy transaction to zero when portfolio value fall to or below zero, this condition is added after several tests
                # are runned on Nov. 20, 2019 and some buy transactions short stocks when portfolio value become negative
                if np.sum(posBook.value) <= 0:
                    cost = 0
                # stop buy when cash fall to or below zero, no leverage. available test results have not considered the below constrain
                elif cash <= 0:
                    cost = 0
                else:
                    cost = np.sum(posBook.value) * inv_prop
                numShares = cost / dat.loc[d,stockcode]
                cash = cash - cost
                posBook.loc['000000','value'] = cash
                posBook.loc[stockcode,['buy_time','numShares','cost','latest_price','value']]=[d,numShares,cost,dat.loc[d,stockcode],cost]
                trans_lst.append([datetime.strftime(d,"%Y-%m-%d"),stockcode,'buy',np.sum(posBook.value),cost,numShares,dat.loc[d,stockcode]])

    testPara.loc[numTest, 'endPortfolio'] = np.sum(posBook.value)
    transBook = pd.DataFrame(trans_lst,columns=['date','stockcode','transaction','portfolio','transAmount','trans_shares','price'])

    transBook.stockcode.astype('str')
    transBook.to_csv('D:/zero/backtest/transBook_%s.csv' % numTest,index=True)
    posBook.to_csv('D:/zero/backtest/posBook_%s.csv' % numTest,index=True)
testPara.to_csv('D:/zero/backtest/posBook_with_results.csv',index=True)
