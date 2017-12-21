'''
Created on 2017年12月21日
基于WIND每日更新行情数据库
@author: 3xtrees
'''
import os
import pandas as pd
import sqlite3
from sqlalchemy import *
import traceback
import datetime

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
        df = pd.read_csv(fileName, encoding="gbk", dtype = {'成交量(股)':str})
        df = df.iloc[:, 2:-1]  # 剔除无用列
        # 成交量为'--'说明当日停牌，（经观察，一字涨跌停成交量也不为0）
        df_filtered = df[(df['成交量(股)'] != '--')]
        # 计算各均线
        ma_list = [5, 10, 20, 30, 60, 120, 240]
        for ma in ma_list:
            df_filtered['MA_' +
                        str(ma)] = df_filtered['收盘价(元)'].rolling(window=ma).mean()
        df_filtered.to_sql(tableName, engine, if_exists='fail', index=False)
        return True
    except Exception:
        traceback.print_exc()
        return False


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    #------------------------------------------------------------------------------ 开始
    engine = create_engine(
        'sqlite:///Stocks_Market_Data_WIND.db', echo=False)  # 创建数据库引擎
    # 读取目录下csv文件，返回文件名和股票列表
    file_name_list, stock_code_list = get_file_name(
        "C:\\Users\\lh159\\Desktop\\dates_new")
    error_list = []
    stocks_num = len(file_name_list)
    for i in range(0, stocks_num):
        print("%d/%d" % ((i + 1), stocks_num))
        result = csv2sqllite(file_name_list[i], stock_code_list[i], engine)
        if not result:
            error_list.append(stock_code_list[i])
    print(error_list)
    #------------------------------------------------------------------------------ 结束
    end_time = datetime.datetime.now()
    print('执行时间:%d minutes' % (int((end_time - start_time).seconds) / 60))
    pass
