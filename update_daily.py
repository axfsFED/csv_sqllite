'''
Created on 2017年12月21日
每日下午4:30更新数据
@author: 3xtrees
'''
import os
import pandas as pd
import sqlite3
from sqlalchemy import *
import traceback
import datetime
from WindPy import *  # 导入wind接口
import datetime
from sqlUtils import *
import logging
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
    now = datetime.datetime.now()
    date_str0 = now.strftime("%Y-%m-%d %H:%M:%S")
    date_str1 = now.strftime("%Y-%m-%d")
    date_str2 = now.strftime("%Y%m%d")

    logging.basicConfig(level=logging.INFO)
    logging.info("开始建立WIND连接")
    w.start()
    logging.info("WIND连接建立成功")
    # 获取历史更新记录
    result = session.query(UPDATE_HISTORY).filter(
        UPDATE_HISTORY.is_latest == 1).all()
    if len(result) > 0:
        update_start_date = result[0].update_date
        logging.info("获取更新历史记录成功:%s" % (update_start_date))
    else:
        logging.info("获取更新历史记录失败，请检查！:%s" % (update_start_date))
        pass

    logging.info("获取当前时间:%s" % (date_str0))

    update_to_date = None
    # 获取上一次更新到现在的交易日列表
    _tds = w.tdays(update_start_date, date_str1, "")
    if len(_tds.Data[0] == 1):
        logging.info("当前数据已为最新，无需更新:%s" % (update_start_date))
        return
    # 判断当天是否是交易日
    _tdc = w.tdayscount(date_str1, date_str1, "")
    if _tdc.Data[0][0] == 0:
        update_to_date = _tds.Data[0][-1]
    else:
        if now.hour > 17:
            logging.info("当前已过17点，数据将更新至今日:%s" % (_tds.Data[0][-1]))
            update_to_date = _tds.Data[0][-1]
        else:
            logging.info("当前未到17点，数据将更新至前一交易日:%s" % (_tds.Data[0][-2]))
            update_to_date = _tds.Data[0][-2]
    logging.info("开始更新数据...:%s")

    _tds = w.tdays(update_start_date, update_to_date, "")
    if len(_tds.Data[0]) > 0:
        print(_tds.Data[0])
        for d in _tds.Data[0][1:]: #对每天的数据进行更新
            _wss = None
            try:
                all_stocks_current = w.wset("sectorconstituent", "date=" +
                                            d + ";sectorid=a001010100000000").Data[1]
                paused_stocks_current = w.wset(
                    "tradesuspend", "startdate=" + d + ";enddate=" + d + ";field=date,wind_code").Data[1]
                un_paused_stocks_current = list(
                    set(all_stocks_current) - set(paused_stocks_current))
                _wss = w.wss(un_paused_stocks_current, "pre_close,open,high,low,close,volume,amt,chg,pct_chg,vwap,turn,mkt_cap_ashare,val_bshrmarketvalue,mkt_cap_ard,float_a_shares,share_liqb,total_shares,pe_lyr,pb_lf,ps_lyr,pcf_nflyr",
                             "tradeDate=" + d + ";priceAdj=F;cycle=D;unit=1")
                df_stocks = pd.DataFrame(
                    _wss.Data, index=_wss.Fields, columns=un_paused_stocks_current).T
                print(df_stocks)
                # 将每一行数据插入对应表中
                for stock_code in df_stocks.index:
                    print(df_stocks.loc[stock_code])
                    # 首先判断数据库中是否存在stock_code表，如果存在的话就插入数据，如果不存在的话就创建对应的表，再插入数据
            except Exception:
                traceback.print_exc()
    else:
        pass
    logging.info("更新数据完成!")
    logging.info("当前WIND获取A股股票个数/数据库中股票个数：%d/%d"%())
    end_time = datetime.datetime.now()
    print('执行时间:%d minutes' % (int((end_time - now).seconds) / 60))