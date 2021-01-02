import baostock as bs
import pandas as pd
import datetime
import pymysql
import time

'''
日线指标参数包括：'date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST'
周、月线指标参数包括：'date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg'
分钟指标参数包括：'date,time,code,open,high,low,close,volume,amount,adjustflag'

adjustflag：复权类型，默认不复权：3；1：后复权；2：前复权。已支持分钟线、日线、周线、月线前后复权。
'''

# 是否删除停盘数据
DROP_SUSPENSION = True

def update_stk_list(date = None):
    # 获取指定日期的指数、股票数据
    stock_rs = bs.query_all_stock(date)
    stock_df = stock_rs.get_data()
    stock_df.to_csv('./stk_data/all_list.csv', encoding = 'gbk', index = False)
    stock_df.drop(stock_df[stock_df.code < 'sh.600000'].index, inplace = True)
    stock_df.drop(stock_df[stock_df.code > 'sz.399000'].index, inplace = True)
    stock_df.to_csv('./stk_data/stk_list.csv', encoding = 'gbk', index = False)
    cursor = db.cursor()

    stock_list_sql = "SELECT * FROM `StockList`"
    cursor.execute(stock_list_sql)
    stock_list = cursor.fetchall()
    stock_tables = pd.DataFrame(stock_list, columns=['code', 'name', 'last_update_date'])

    for index, data in stock_df.iterrows():
        if data.code in stock_tables['code'].values:
            continue

        sql = 'INSERT INTO `StockList` (`id`, `name`, `last_update_date`) VALUES (\'{stockId}\', \'{stockName}\', \'{stockTime}\')'\
                .format(stockId=data.code, stockName=data.code_name, stockTime='1970-01-01')
        try:
            cursor.execute(sql)
            db.commit()
        except Exception as e:
            print('failed', e)
            db.rollback()

    stock_list_sql = "SELECT * FROM `StockList`"
    cursor.execute(stock_list_sql)
    stock_list = cursor.fetchall()
    stock_tables = pd.DataFrame(stock_list, columns=['code', 'name', 'last_update_date'])
    return stock_tables

def load_stk_list():
    df = pd.read_csv('./stk_data/stk_list.csv')
    return df['code'].tolist()

def convert_time(t):
    H = t[8:10]
    M = t[10:12]
    S = t[12:14]
    return H + ':' + M + ':' + S

def compare_time(time1,time2):
    s_time = time.mktime(time.strptime(time1,'%Y-%m-%d'))
    e_time = time.mktime(time.strptime(time2,'%Y-%m-%d'))
    return int(s_time) - int(e_time)

def download_data(stk_list, fromdate = '2020-01-01', todate = datetime.date.today(),
                   datas = 'date,open,high,low,close,volume,amount,turn,pctChg',
                   frequency = 'd', adjustflag = '2'):
    for index, stockData in stk_list.iterrows():
        print("Downloading :" + stockData.code)
        diff_time = abs(compare_time(stockData.last_update_date.strftime('%Y-%m-%d'), datetime.date.today().strftime('%Y-%m-%d')))
        if diff_time <= 0:
            continue

        k_rs = bs.query_history_k_data_plus(stockData.code, datas, start_date = fromdate, end_date = todate.strftime('%Y-%m-%d'),
                                            frequency = frequency, adjustflag = adjustflag)
        datapath = './stk_data/' + frequency + '/' + stockData.code + '.csv'

        cursor = db.cursor()
        out_df = k_rs.get_data()
        if DROP_SUSPENSION and 'volume' in list(out_df):
            out_df.drop(out_df[out_df.volume == '0'].index, inplace = True)

        max_update_time = stockData.last_update_date
        for index, data in out_df.iterrows():
            diff_time_when_insert = compare_time(data.date, stockData.last_update_date.strftime('%Y-%m-%d'));
            if diff_time_when_insert > 0:
                max_update_time = data.date

            sql = 'INSERT INTO `StockData` (`id`, `time`, `open`, `high`, `low`, `close`, `volume`, `turn`, `pctChg`) VALUES (\'{stockId}\', \'{stockTime}\', {stockOpen}, {stockHigh}, {stockLow}, {stockClose}, {stockVolume}, {stockTurn}, {stockPcgChg})'\
                .format(stockId=stockData.code, stockTime=data.date, stockOpen=data.open, stockHigh=data.high, stockLow=data.low, stockClose=data.close, stockVolume=data.volume, stockTurn=data.turn, stockPcgChg=data.pctChg)
            try:
                cursor.execute(sql)
                db.commit()
            except Exception as e:
                db.rollback()
        out_df.to_csv(datapath, encoding = 'gbk', index = False)

        # 更新最后一次的更新时间
        update_sql = 'UPDATE `StockList` SET `last_update_date` = \'{stockTime}\' WHERE `id` = \'{stockId}\''\
            .format(stockId=stockData.code, stockTime=max_update_time)
        try:
            cursor.execute(update_sql)
            db.commit()
        except Exception as e:
            db.rollback()
    db.close()

if __name__ == '__main__':
    bs.login()

    db = pymysql.connect("localhost", "root", "", "Stocks")

    # 首次运行
    stk_list = update_stk_list(datetime.date.today() - datetime.timedelta(days = 31))

    # 下载日线
    download_data(stk_list)
    bs.logout()

