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
    #now = datetime.datetime(2017, 12, 27)
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
        sys.exit()

    logging.info("获取当前时间:%s" % (date_str0))

    update_to_date = None
    # 获取上一次更新到现在的交易日列表
    _tds = w.tdays(update_start_date, date_str1, "")
    if len(_tds.Data[0]) == 1:
        logging.info("当前数据已为最新，无需更新:%s" % (update_start_date))
        sys.exit()
    # 判断当天是否是交易日
    _tdc = w.tdayscount(date_str1, date_str1, "")
    if _tdc.Data[0][0] == 0:
        update_to_date = _tds.Data[0][-1]
    else:  # 如果当天是交易日
        if now.hour > 17:
            logging.info("当前已过17点，数据将更新至今日:%s" % (_tds.Data[0][-1]))
            update_to_date = _tds.Data[0][-1]
        else:
            logging.info("当前未到17点，数据将更新至前一交易日:%s" % (_tds.Data[0][-2]))
            update_to_date = _tds.Data[0][-2]
    logging.info("开始更新数据...")

    _tds = w.tdays(update_start_date, update_to_date, "")
    for date in _tds.Data[0][1:2]:  # 对每天的数据进行更新
        d_str1 = date.strftime("%Y-%m-%d")
        d_str2 = date.strftime("%Y%m%d")
        try:
            all_stocks_current = w.wset("sectorconstituent", "date=" +
                                        d_str1 + ";sectorid=a001010100000000").Data[1]
            paused_stocks_current = w.wset(
                "tradesuspend", "startdate=" + d_str1 + ";enddate=" + d_str1 + ";field=date,wind_code").Data[1]
            un_paused_stocks_current = list(
                set(all_stocks_current) - set(paused_stocks_current))
            _wss = w.wss('000001.SZ', "pre_close,open,high,low,close,volume,amt,chg,pct_chg,vwap,turn,mkt_cap_ashare,val_bshrmarketvalue,mkt_cap_ard,float_a_shares,share_liqb,total_shares,pe_lyr,pb_lf,ps_lyr,pcf_nflyr",
                         "tradeDate=" + d_str2 + ";priceAdj=F;cycle=D;unit=1")
            df_stocks = pd.DataFrame(
                _wss.Data, index=_wss.Fields, columns=['000001.SZ']).T
            df_stocks.rename(
                columns={'PRE_CLOSE': u'前收盘价(元)', 'OPEN': u'开盘价(元)', 'HIGH': u'最高价(元)', 'LOW': u'最低价(元)', 'CLOSE': u'收盘价(元)', 'VOLUME': u'成交量(股)', 'AMT': u'成交金额(元)', 'CHG': u'涨跌(元)', 'PCT_CHG': u'涨跌幅(%)', 'VWAP': u'均价(元)', 'TURN': u'换手率(%)', 'MKT_CAP_ASHARE': u'A股流通市值(元)', 'VAL_BSHRMARKETVALUE': u'B股流通市值(元)', 'MKT_CAP_ARD': u'总市值(元)', 'FLOAT_A_SHARES': u'A股流通股本(股)', 'SHARE_LIQB': u'B股流通股本(股)', 'TOTAL_SHARES': u'总股本(股)', 'PE_LYR': u'市盈率', 'PB_LF': u'市净率', 'PS_LYR': u'市销率', 'PCF_NFLYR': u'市现率'}, inplace=True)
            # 获取均线数据
            MA_5 = w.wss("000001.SZ", "MA", "tradeDate=" +
                         d_str2 + ";MA_N=5;priceAdj=F;cycle=D").Data[0][0]
            MA_10 = w.wss("000001.SZ", "MA", "tradeDate=" +
                          d_str2 + ";MA_N=10;priceAdj=F;cycle=D").Data[0][0]
            MA_20 = w.wss("000001.SZ", "MA", "tradeDate=" +
                          d_str2 + ";MA_N=20;priceAdj=F;cycle=D").Data[0][0]
            MA_30 = w.wss("000001.SZ", "MA", "tradeDate=" +
                          d_str2 + ";MA_N=30;priceAdj=F;cycle=D").Data[0][0]
            MA_60 = w.wss("000001.SZ", "MA", "tradeDate=" +
                          d_str2 + ";MA_N=60;priceAdj=F;cycle=D").Data[0][0]
            MA_120 = w.wss("000001.SZ", "MA", "tradeDate=" +
                           d_str2 + ";MA_N=120;priceAdj=F;cycle=D").Data[0][0]
            MA_240 = w.wss("000001.SZ", "MA", "tradeDate=" +
                           d_str2 + ";MA_N=240;priceAdj=F;cycle=D").Data[0][0]

            df_stocks['日期'] = [d_str1 for idx in df_stocks.index]
            df_stocks['MA_5'] = MA_5
            df_stocks['MA_10'] = MA_10
            df_stocks['MA_20'] = MA_20
            df_stocks['MA_30'] = MA_30
            df_stocks['MA_60'] = MA_60
            df_stocks['MA_120'] = MA_120
            df_stocks['MA_240'] = MA_240
            
            df_stocks = df_stocks.fillna('--')
            # 将每一行数据插入对应表中
            # for stock_code in df_stocks.index:
            print(df_stocks.loc['000001.SZ'])
            # 首先判断数据库中是否存在stock_code表，如果存在的话就插入数据，如果不存在的话就创建对应的表，再插入数据
            df_stocks.to_sql('000001.SZ', engine,
                             if_exists='append', index=False)
        except Exception:
            traceback.print_exc()
    logging.info("更新数据完成!")
    logging.info("当前WIND获取A股股票个数/数据库中股票个数：%d/%d")
    end_time = datetime.datetime.now()
    print('执行时间:%d minutes' % (int((end_time - now).seconds) / 60))
