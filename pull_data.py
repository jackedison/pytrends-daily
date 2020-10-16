import pytrend_daily
import pandas as pd
from pytrends.request import TrendReq
from datetime import datetime

def pull_data_to_csv(keyword):
    pytrend = TrendReq(hl='en-US')
    keyword = keyword
    start = '2019-01-01'
    end = '2020-10-15'
    geo = 'US'
    cat = 0
    gprop = ''

    overlapping = pytrend_daily.get_daily_trend(pytrend, keyword, start, end, geo=geo, cat=cat, gprop=gprop, verbose=True, tz=0)

    start = '2020-03-01'
    end = '2020-10-15'
    kw_list = [keyword]
    start_d = datetime.strptime(start, '%Y-%m-%d')
    end_d = datetime.strptime(end, '%Y-%m-%d')
    tf = start_d.strftime('%Y-%m-%d')+' '+end_d.strftime('%Y-%m-%d')

    daily = pytrend_daily.fetch_data(pytrend, kw_list, timeframe=tf,)

    overlapping.to_csv('{}_overlapping.csv'.format(keyword))
    daily.to_csv('{}_daily.csv'.format(keyword))
