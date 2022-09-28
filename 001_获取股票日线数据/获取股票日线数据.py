import pandas as pd
from struct import unpack
import os
import akshare as ak
import baostock as bs
from opendatatools import stock

# 将通达信日线数据转换为 dataframe
# 注意，导出的数据未复权！
def TDX2df(file_name):
    '''
    00 ~ 03 字节：年月日, 整型
    04 ~ 07 字节：开盘价*100，整型
    08 ~ 11 字节：最高价*100，整型
    12 ~ 15 字节：最低价*100，整型
    16 ~ 19 字节：收盘价*100，整型
    20 ~ 23 字节：成交额（元），float型
    24 ~ 27 字节：成交量（股），整型
    28 ~ 31 字节：（保留）
    '''
    df = []
    with open(file_name,'rb') as f:
        buffer = f.read()
    size = len(buffer)
    row_size = 32 # 通达信的 .day 数据，每 32 个字节为一组数据
    file = os.path.basename(file_name).replace('.day','')
    for i in range(0,size,row_size):
        row = list(unpack('IIIIIfII',buffer[i:i+row_size]))
        row[1] = row[1] / 100
        row[2] = row[2] / 100
        row[3] = row[3] / 100
        row[4] = row[4] / 100
        row.pop()
        row.insert(0,file)
        df.append(row)
        
    df = pd.DataFrame(data=df,columns=['股票代码','交易日','开盘价','最高价','最低价','收盘价','成交额','成交量'])
    df['交易日'] = df['交易日'].astype('str')
    df = df[(df['交易日'] >= '20120301') & (df['交易日'] <= '20120315')]
    df['交易日'] = pd.to_datetime(df['交易日']).dt.date
    print(f'将通达信日线数据转换为 dataframe:\n{df}')
    df.to_excel('./TDX_day.xlsx',index=False)
    print('成功导出 Excel 文件!')


# 将 akshare 日线数据转换为 dataframe
def akshare2df():
    '''
    数据来源：东方财富网
    
    名称	类型	描述
    symbol	    str	symbol='603777'; 股票代码可以在 ak.stock_zh_a_spot_em() 中获取
    period	    str	period='daily'; choice of {'daily', 'weekly', 'monthly'}
    start_date	str	start_date='20210301'; 开始查询的日期
    end_date	str	end_date='20210616'; 结束查询的日期
    adjust	    str	默认返回不复权的数据; qfq: 返回前复权后的数据; hfq: 返回后复权后的数据
    
    名称	类型	描述
    日期	object	交易日
    开盘	float64	开盘价
    收盘	float64	收盘价
    最高	float64	最高价
    最低	float64	最低价
    成交量	int32	注意单位: 手
    成交额	float64	注意单位: 元
    振幅	float64	注意单位: %
    涨跌幅	float64	注意单位: %
    涨跌额	float64	注意单位: 元
    换手率	float64	注意单位: %
    '''
    stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol='600006', period='daily', start_date='20120301', end_date='20120315', adjust='')
    print(f'将 akshare 日线数据转换为 dataframe:\n{stock_zh_a_hist_df}')
    stock_zh_a_hist_df.to_excel('./akshare_day.xlsx',index=False)
    print('成功导出 Excel 文件!')
    
 
# 将 baostock 日线数据转换为 dataframe
def baostock2df():
    '''
    code：股票代码，sh或sz.+6位数字代码，或者指数代码，如：sh.601398。sh：上海；sz：深圳。此参数不可为空；
    fields：指示简称，支持多指标输入，以半角逗号分隔，填写内容作为返回类型的列。详细指标列表见历史行情指标参数章节，日线与分钟线参数不同。此参数不可为空；
    start：开始日期（包含），格式“YYYY-MM-DD”，为空时取2015-01-01；
    end：结束日期（包含），格式“YYYY-MM-DD”，为空时取最近一个交易日；
    frequency：数据类型，默认为d，日k线；d=日k线、w=周、m=月、5=5分钟、15=15分钟、30=30分钟、60=60分钟k线数据，不区分大小写；指数没有分钟线数据；周线每周最后一个交易日才可以获取，月线每月最后一个交易日才可以获取。
    adjustflag：复权类型，默认不复权：3；1：后复权；2：前复权。已支持分钟线、日线、周线、月线前后复权。 BaoStock提供的是涨跌幅复权算法复权因子
    
    参数名称	参数描述	算法说明
    date	    交易所行情日期	
    code	    证券代码	
    open	    开盘价	
    high	    最高价	
    low	        最低价	
    close	    收盘价	
    preclose	前收盘价
    volume	    成交量（累计 单位：股）	
    amount	    成交额（单位：人民币元）	
    adjustflag	复权状态(1：后复权， 2：前复权，3：不复权）	
    turn	    换手率	[指定交易日的成交量(股)/指定交易日的股票的流通股总股数(股)]*100%
    tradestatus	交易状态(1：正常交易 0：停牌）	
    pctChg	    涨跌幅（百分比）	日涨跌幅=[(指定交易日的收盘价-指定交易日前收盘价)/指定交易日前收盘价]*100%
    peTTM	    滚动市盈率	(指定交易日的股票收盘价/指定交易日的每股盈余TTM)=(指定交易日的股票收盘价*截至当日公司总股本)/归属母公司股东净利润TTM
    pbMRQ	    市净率	(指定交易日的股票收盘价/指定交易日的每股净资产)=总市值/(最近披露的归属母公司股东的权益-其他权益工具)
    psTTM	    滚动市销率	(指定交易日的股票收盘价/指定交易日的每股销售额)=(指定交易日的股票收盘价*截至当日公司总股本)/营业总收入TTM
    pcfNcfTTM	滚动市现率	(指定交易日的股票收盘价/指定交易日的每股现金流TTM)=(指定交易日的股票收盘价*截至当日公司总股本)/现金以及现金等价物净增加额TTM
    isST	    是否ST股，1是，0否	
    
    参数名称	参数描述	说明
    date	交易所行情日期	格式：YYYY-MM-DD
    code	证券代码	格式：sh.600000。sh：上海，sz：深圳
    open	今开盘价格	精度：小数点后4位；单位：人民币元
    high	最高价	精度：小数点后4位；单位：人民币元
    low	    最低价	精度：小数点后4位；单位：人民币元
    close	今收盘价	精度：小数点后4位；单位：人民币元
    preclose	昨日收盘价	精度：小数点后4位；单位：人民币元
    volume	成交数量	单位：股
    amount	成交金额	精度：小数点后4位；单位：人民币元
    adjustflag	复权状态	不复权、前复权、后复权
    turn	换手率	精度：小数点后6位；单位：%
    tradestatus	交易状态	1：正常交易 0：停牌
    pctChg	涨跌幅（百分比）	精度：小数点后6位
    peTTM	滚动市盈率	精度：小数点后6位
    psTTM	滚动市销率	精度：小数点后6位
    pcfNcfTTM	滚动市现率	精度：小数点后6位
    pbMRQ	市净率	精度：小数点后6位
    isST	是否ST	1是，0否
    '''
    lg = bs.login()
    print('login respond error_code:' + lg.error_code)
    print('login respond  error_msg:' + lg.error_msg) 
    
    rs = bs.query_history_k_data_plus('sh.600006','date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST',start_date='2012-03-01', end_date='2012-03-15',frequency='d', adjustflag='3')
    # print('query_history_k_data_plus respond error_code:' + rs.error_code)
    # print('query_history_k_data_plus respond  error_msg:' + rs.error_msg)

    baostock_dt = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        baostock_dt.append(rs.get_row_data())
    result = pd.DataFrame(baostock_dt, columns=rs.fields)
    print(f'将 baostock 日线数据转换为 dataframe:\n{result}')
    result.to_excel('./baostock_day.xlsx',index=False)
    print('成功导出 Excel 文件!')
    
    bs.logout()
    

# 将 opendatatools 日线数据转换为 dataframe
# 注意，导出的数据是前复权的！
def opendatatools2df():
    '''
    数据来源：雪球网
    '''
    opendatatools_df, msg = stock.get_daily('600006.SH', start_date='2012-03-01', end_date='2012-03-15')
    print(msg)
    print(f'将 opendatatools 日线数据转换为 dataframe:\n{opendatatools_df}')
    opendatatools_df.to_excel('./opendatatools_day.xlsx',index=False)
    print('成功导出 Excel 文件!')
 
   
def main():
    file_name = './sh600006.day'
    TDX2df(file_name)
    akshare2df()
    baostock2df()
    opendatatools2df()
    
    
if __name__=='__main__':
    main()