import tushare as ts
import pandas as pd
import matplotlib.pyplot as plt

def get_data(list_file):
    
    pro = ts.pro_api()

    reit_file = open('reits_list.txt')

    for line in reit_file.readlines():
        code = line.split(',')[0].strip()
        exchange = line.split(',')[1].strip()
        ts_code = '.'.join([code, exchange])
        price = pro.fund_daily(**{"trade_date": "",
                                  "start_date": "",
                                  "end_date": "",
                                  "ts_code": ts_code,
                                  "limit": "",
                                  "offset": ""
                                  }, 
                                  fields=["ts_code",
                                          "trade_date",
                                          "pre_close",
                                          "open",
                                          "high",
                                          "low",
                                          "close",
                                          "change",
                                          "pct_chg",
                                          "vol",
                                          "amount"
                                          ]
                               )
        price.to_csv(f'Data/{exchange}_{code}_price.csv',
                     index=False, encoding='utf_8_sig')
    
    reit_file.close()


def get_fund_shares(list_file):

    pro = ts.pro_api()
    reit_file = open('reits_list.txt')
    
    for line in reit_file.readlines():
        code = line.split(',')[0].strip()
        exchange = line.split(',')[1].strip()
        ts_code = '.'.join([code, exchange])    
        shares = pro.fund_share(**{
                                "ts_code": ts_code,
                                "trade_date": "",
                                "start_date": "",
                                "end_date": "",
                                "market": "",
                                "fund_type": "",
                                "limit": "",
                                "offset": ""
                                }, 
                                fields=["ts_code",
                                        "trade_date",
                                        "fd_share",
                                        "fund_type",
                                        "market",
                                        "total_share"
                                        ]
                            )
        shares.to_csv(f'Data/{exchange}_{code}_shares.csv',
                        index=False, encoding='utf_8_sig')
        
    reit_file.close()

def get_dividend(list_file):
    pro = ts.pro_api()
    reit_file = open('reits_list.txt')
    
    for line in reit_file.readlines():
        code = line.split(',')[0].strip()
        exchange = line.split(',')[1].strip()
        ts_code = '.'.join([code, exchange])    
        dividend = pro.fund_div(**{"ann_date": "",
                                  "ex_date": "",
                                  "pay_date": "",
                                  "ts_code": ts_code,
                                  "limit": "",
                                  "offset": ""
                                 },
                                 fields=["ts_code",
                                          "ann_date",
                                          "imp_anndate",
                                          "base_date",
                                          "div_proc",
                                          "record_date",
                                          "ex_date",
                                          "pay_date",
                                          "earpay_date",
                                          "net_ex_date",
                                          "div_cash",
                                          "base_unit",
                                          "ear_distr",
                                          "ear_amount",
                                          "account_date",
                                          "base_year"
                                        ]
                              )
        dividend.to_csv(f'Data/{exchange}_{code}_dividend.csv',
                        index=False, encoding='utf_8_sig')


def plot_price(code, exchange):
    code = str(code)
    dat = pd.read_csv(f'Data/{exchange}_{code}_price.csv',
                      index_col=1, parse_dates=[1])
    
    fig = plt.figure(figsize=(12,8))
    fig.suptitle(f'Price and Volume:{code}')
    top = plt.subplot2grid((5,1),(0,0),
                           rowspan=3, colspan=4)
    top.plot(dat.index, dat['close'])
    top.set_title(f"""From {dat.index[0].date()} to {dat.index[-1].date()}""")
    top.set_xticks([])
    top.set_xlabel('')
    
    bottom = plt.subplot2grid((5,1),(3,0),
                               rowspan=1, colspan=4)
    bottom.bar(dat.index, dat['vol'])
    bottom.set_xlabel('Date', loc='right')
    plt.tight_layout()
    plt.show()

# get_data('reits_list.txt')
# get_fund_shares('reits_list.txt')
# get_dividend('reits_list.txt')
reit_file = open('reits_list.txt')
for line in reit_file.readlines():
    code = line.split(',')[0].strip()
    exchange = line.split(',')[1].strip()
    plot_price(code, exchange)
