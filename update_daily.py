'''
Created on 2017年12月21日

@author: 3xtrees
'''
import os
import pandas as pd
import sqlite3
from sqlalchemy import *
import traceback
import datetime
from WindPy import *  # 导入wind接口

#=========================================================================
# 获取指定目录下所有的csv文件
#=========================================================================


def get_file_name(file_dir):
    file_name_list = []
    stock_code_list = []
    for root, dirs, files in os.walk(file_dir):
        for file in files:
            if os.path.splitext(file)[1] == '.CSV':
                file_name_list.append(os.path.join(root, file))
                stock_code_list.append(os.path.splitext(file)[0])
    return file_name_list, stock_code_list

#=========================================================================
# 将csv文件写入sqllite数据库
#=========================================================================


def csv2sqllite(fileName, tableName, engine):
    # 将pandas读入csv，停牌剔除，并计算均线等基本技术指标后，写入sqllite数据库
    try:
        df = pd.read_csv(fileName, encoding="gbk")
        df = df.iloc[:, 2:-1]  # 剔除无用列
        # 成交量为'--'说明当日停牌，（经观察，一字涨跌停成交量也不为0）
        df_filtered = df[(df['成交量(股)'] != '--')]
        # 计算各均线
        ma_list = [5, 10, 20, 30, 60, 120, 240]
        for ma in ma_list:
            df_filtered['MA_' +
                        str(ma)] = df_filtered['收盘价(元)'].rolling(window=ma).mean()
        df_filtered.to_sql(tableName, engine, if_exists='replace', index=False)
        return True
    except BaseException:
        return False


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    #------------------------------------------------------------------------------ 开始 
    w.start()
    now = datetime.datetime.now()
    date_str1 = now.strftime("%Y-%m-%d")
    date_str2 = now.strftime("%Y%m%d")
    #------------------------------------------------------------------------- 获取当天A股全体股票列表和停牌股票
    _wss = None
    try:
        all_stocks_current = w.wset("sectorconstituent", "date=" +
                             date_str1 + ";sectorid=a001010100000000").Data[1]
        paused_stocks_current = w.wset("tradesuspend","startdate="+date_str1+";enddate="+date_str1+";field=date,wind_code").Data[1]
        un_paused_stocks_current = list(set(all_stocks_current) - set(paused_stocks_current))
        _wss = w.wss(un_paused_stocks_current, "pre_close,open,high,low,close,volume,amt,chg,pct_chg,vwap,turn,mkt_cap_ashare,val_bshrmarketvalue,mkt_cap_ard,float_a_shares,share_liqb,total_shares,pe_lyr,pb_lf,ps_lyr,pcf_nflyr",
              "tradeDate=20171219;priceAdj=F;cycle=D;unit=1")
        df_stocks = pd.DataFrame(_wss.Data, index=_wss.Fields,columns=all_stocks_current).T
        print(df_stocks)
    except Exception:
        traceback.print_exc()
    #------------------------------------------------------------------------------ 结束
    end_time = datetime.datetime.now()
    print('执行时间:%d minutes' % (int((end_time - start_time).seconds) / 60))
    pass
