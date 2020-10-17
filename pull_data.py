import pytrend_daily
import pandas as pd
from pytrends.request import TrendReq
from datetime import datetime

def pull_data_to_csv(keyword, start, end, geo, cat, gprop, method, verbose):
    pytrend = TrendReq(hl='en-US')

    # Overlapping (ensure more than 270 days)
    overlapping = pytrend_daily.get_daily_trend(pytrend, keyword, start, end,
                                                method=method, geo=geo, cat=cat,
                                                gprop=gprop, verbose=verbose)

    # Weekly data
    kw_list = [keyword]
    tf = start+' '+end

    weekly = pytrend_daily.fetch_data(pytrend, kw_list, timeframe=tf,)

    # Daily range to compare (ensure less than 270 days)
    start = '2020-03-01'
    end = '2020-10-15'
    tf = start+' '+end

    daily = pytrend_daily.fetch_data(pytrend, kw_list, timeframe=tf,)

    # Save as csvs
    overlapping.to_csv('data/{}_overlapping.csv'.format(keyword))
    weekly.to_csv('data/{}_weekly.csv'.format(keyword))
    daily.to_csv('data/{}_daily.csv'.format(keyword))
