'''
Created on 2017年12月13日

@author: 3xtrees
'''
from sqlalchemy import Column, String, Integer, Float, create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import gevent


engine = create_engine(
    'sqlite:///Stocks_Market_Data_WIND.db', echo=False)  # 创建数据库引擎

# 创建DBSession类型，以及session会话
DBSession = sessionmaker(bind=engine)
session = DBSession()

# 创建对象的基类:
Base = declarative_base()
#=========================================================================
# 定义数据更新历史表
#=========================================================================


class UPDATE_HISTORY(Base):
    # 表的名字:
    __tablename__ = 'update_history'
    # 表的结构:
    update_date = Column(String(), primary_key=True)
    is_latest = Column(Integer)


#=========================================================================
# 定义通用类，股票信息表-->类映射
#=========================================================================


def get_class(table_name):
    Table(table_name, Base.metadata,
          Column('日期', String, primary_key=True),
          Column('前收盘价(元)', String),
          Column('开盘价(元)', Float),
          Column('最高价(元)', Float),
          Column('最低价(元)', Float),
          Column('收盘价(元)', Float),
          Column('成交量(股)', String),
          Column('成交金额(元)', String),
          Column('涨跌(元)', String),
          Column('涨跌幅(%)', String),
          Column('均价(元)', String),
          Column('换手率(%)', String),
          Column('A股流通市值(元)', Float),
          Column('B股流通市值(元)', String),
          Column('总市值(元)', Float),
          Column('A股流通股本(股)', Integer),
          Column('B股流通股本(股)', Integer),
          Column('总股本(股)', Integer),
          Column('市盈率', Float),
          Column('市净率', Float),
          Column('市销率', String),
          Column('市现率', String),
          Column('MA_5', Float),
          Column('MA_10', Float),
          Column('MA_20', Float),
          Column('MA_30', Float),
          Column('MA_60', Float),
          Column('MA_120', Float),
          Column('MA_240', Float)
          )

    class GenericTable(Base):
        __table__ = Base.metadata.tables[table_name]
        # 表的结构:
        date = __table__.c['日期']
        pre_close = __table__.c['前收盘价(元)']
        open = __table__.c['开盘价(元)']
        high = __table__.c['最高价(元)']
        low = __table__.c['最低价(元)']
        close = __table__.c['收盘价(元)']
        volume = __table__.c['成交量(股)']
        amt = __table__.c['成交金额(元)']
        chg = __table__.c['涨跌(元)']
        pct_chg = __table__.c['涨跌幅(%)']
        vwap = __table__.c['均价(元)']
        turn = __table__.c['换手率(%)']
        mkt_cap_ashare = __table__.c['A股流通市值(元)']
        val_bshrmarketvalue = __table__.c['B股流通市值(元)']
        mkt_cap_ard = __table__.c['总市值(元)']
        float_a_shares = __table__.c['A股流通股本(股)']
        share_liqb = __table__.c['B股流通股本(股)']
        total_shares = __table__.c['总股本(股)']
        pe_lyr = __table__.c['市盈率']
        pb_lf = __table__.c['市净率']
        ps_lyr = __table__.c['市销率']
        pcf_nflyr = __table__.c['市现率']
        MA_5 = __table__.c['MA_5']
        MA_10 = __table__.c['MA_10']
        MA_20 = __table__.c['MA_20']
        MA_30 = __table__.c['MA_30']
        MA_60 = __table__.c['MA_60']
        MA_120 = __table__.c['MA_120']
        MA_240 = __table__.c['MA_240']
    return GenericTable




#test = get_class('000001.SZ')
#new_record = test(update_date='2017-12-21', is_latest=1)
#new_record = UPDATE_HISTORY(update_date='2017-12-21', is_latest=1)
# session.add(new_record)
# session.commit()


# 条件查询和更改
# result = session.query(test).filter(
#     test.date == '2017-12-20').all()
# if len(result) > 0:
#     print(result[0].total_shares)
# for h in hi:
#     setattr(h, 'selected_code', 'updated')
#     session.commit()


# # 插入记录
# zztk = ZZTK(selected_date='test', selected_code='test', buy_date='test',
#             buy_price=10, sell_date='test', sell_price=11, shares=100)
# try:
#     session.add(zztk)
#     session.commit()
# except gevent.Timeout:
#     session.invalidate()
#     raise
# except:
#     session.rollback()
#     raise
