#!/usr/bin/env python
# coding: utf-8


from datetime import datetime, timedelta, date, time
import pandas as pd
import time

from pytrends.request import TrendReq
from pytrends.exceptions import ResponseError


def fetch_data(trendreq, kw_list, timeframe='today 3-m', cat=0, geo='', gprop='') -> pd.DataFrame:
    
    """Download google trends data using pytrends TrendReq and retries in case of a ResponseError."""
    attempts, fetched = 0, False
    while not fetched:
        try:
            trendreq.build_payload(kw_list=kw_list, timeframe=timeframe, cat=cat, geo=geo, gprop=gprop)
        except ResponseError as err:
            print(err)
            print(f'Trying again in {60 + 5 * attempts} seconds.')
            time.sleep(60 + 5 * attempts)
            attempts += 1
            if attempts > 3:
                print('Failed after 3 attemps, abort fetching.')
                break
        else:
            fetched = True
    return trendreq.interest_over_time()

def get_daily_trend(trendreq, keyword:str, start:str, end:str, cat=0, 
                    geo='', gprop='', delta=269, overlap=100, sleep=0, 
                    tz=0, verbose=False) ->pd.DataFrame:

    """Stich and scale consecutive daily trends data between start and end date.
    This function will first download piece-wise google trends data and then 
    scale each piece using the overlapped period. 

        Parameters
        ----------
        trendreq : TrendReq
            a pytrends TrendReq object
        keyword: str
            currently only support single keyword, without bracket
        start: str
            starting date in string format:YYYY-MM-DD (e.g.2017-02-19)
        end: str
            ending date in string format:YYYY-MM-DD (e.g.2017-02-19)
        cat, geo, gprop, sleep: 
            same as defined in pytrends
        delta: int
            The length(days) of each timeframe fragment for fetching google trends data, 
            need to be <269 in order to obtain daily data.
        overlap: int
            The length(days) of the overlap period used for scaling/normalization
        tz: int
            The timezone shift in minute relative to the UTC+0 (google trends default).
            For example, correcting for UTC+8 is 480, and UTC-6 is -360 

    """
    
    # Set start dates, end dates, and date period jump in queryable formats
    start_d = datetime.strptime(start, '%Y-%m-%d')
    init_end_d = end_d = datetime.strptime(end, '%Y-%m-%d')
    init_end_d.replace(hour=23, minute=59, second=59)    
    delta = timedelta(days=delta)  # default delta 269 days
    overlap = timedelta(days=overlap)  # default overlap of 100 days

    itr_d = end_d - delta
    overlap_start = None

    df = pd.DataFrame()
    ol = pd.DataFrame()
    
    # Loop through while end date is further than start date
    while end_d > start_d:
        # Call google for data of our current data range
        tf = itr_d.strftime('%Y-%m-%d')+' '+end_d.strftime('%Y-%m-%d')
        if verbose: print('Fetching \''+keyword+'\' for period:'+tf)
        temp = fetch_data(trendreq, [keyword], timeframe=tf, cat=cat, geo=geo, gprop=gprop)

        # Fix the data by removing isPartial column and call data col the date range
        temp.drop(columns=['isPartial'], inplace=True)
        temp.columns.values[0] = tf

        # Create a copy of the data and remove all data values. So skeleton db of dates
        ol_temp = temp.copy()
        ol_temp.iloc[:,:] = None

        # If we need to overlap past data then:
        if overlap_start is not None:  # not first iteration
            if verbose: print('Normalize by overlapping period:'+overlap_start.strftime('%Y-%m-%d'), end_d.strftime('%Y-%m-%d'))
            
            # Normalize using the maximum value of the overlapped period
            # Y1 is max data value in our new data in the 100 day overlap period
            y1 = temp.loc[overlap_start:end_d].iloc[:,0].values.max()

            # Y2 is the max data in our existing data in the 100 day overlap period
            y2 = df.loc[overlap_start:end_d].iloc[:,-1].values.max()

            # Take the ratio of the two max vals and multiple our new data by it
            coef = y2/y1
            temp = temp * coef

            # To our skeleton date df set =1 in a column headed with the overlap period (Empty if not)
            ol_temp.loc[overlap_start:end_d, :] = 1 

        # Concatenate the main df with our new, temp data. Note new col labeled with date range so won't replace old data with new normalised.
        df = pd.concat([df,temp], axis=1)
        # Concatenate the overlap df with our new overlap data??
        ol = pd.concat([ol, ol_temp], axis=1)

        # Shift the timeframe for next iteration
        overlap_start = itr_d
        end_d -= (delta-overlap)
        itr_d -= (delta-overlap)

        # in case of short query interval getting banned by server
        time.sleep(sleep)
    
    # Once loop is completed we will have:
    # 1. df: has all data normalised based on max values over ol period
    # 2. ol: has all dates we fetched data for with a 1 if it was overlapped
    df.sort_index(inplace=True)
    ol.sort_index(inplace=True)


    # If we are fetching until today, the daily trend data will not yet have updated withthe most recent 3 days of data
    # So we will need to fetch it with hourly data and normalise
    if df.index.max() < init_end_d : 
        # Get data for most recent 7 days with the 'now 7-d' timeframe parameter. 4 days to normalise, 3 days new data.
        tf = 'now 7-d'
        hourly = fetch_data(trendreq, [keyword], timeframe=tf, cat=cat, geo=geo, gprop=gprop)
        hourly.drop(columns=['isPartial'], inplace=True)
        
        # Convert hourly data to daily data by summing every hour into a day row
        daily = hourly.groupby(hourly.index.date).sum()
        
        # Check whether the first day data is complete (i.e. has 24 hours)
        # @JACK - shouldn't this drop last row? .iloc[-1]
        daily['hours'] = hourly.groupby(hourly.index.date).count()
        if daily.iloc[0].loc['hours'] != 24: daily.drop(daily.index[0], inplace=True)
        daily.drop(columns='hours', inplace=True)
        
        # Set index to the dates and the column header to 'now 7-d'
        daily.set_index(pd.DatetimeIndex(daily.index), inplace=True)
        daily.columns = [tf]

        # Copy the dates to ol_temp and remove the data. Skeleton copy for ol    
        ol_temp = daily.copy()
        ol_temp.iloc[:,:] = None

        # Find the overlapping dates
        intersect = df.index.intersection(daily.index)
        if verbose: print('Normalize by overlapping period:'+(intersect.min().strftime('%Y-%m-%d'))+' '+(intersect.max().strftime('%Y-%m-%d')))

        # Normalise the new data using the overlapped period of today-4 to today-7. Again max values.
        coef = df.loc[intersect].iloc[:,0].max() / daily.loc[intersect].iloc[:,0].max()
        daily = (daily*coef).round(decimals=0)
        ol_temp.loc[intersect,:] = 1
        
        # Add the final 3 days to our data
        df = pd.concat([daily, df], axis=1)
        ol = pd.concat([ol_temp, ol], axis=1)

    # For our overlapped periods, we will take the mean of our the pulled data, and our adjusted pulled data with the coefficients. Why? Idk but it works..
    df = df.mean(axis=1)
    ol = ol.max(axis=1)

    # Then merge the two dataframe (trend data and flag of whether it was overlapped)
    df = pd.concat([df,ol], axis=1)
    df.columns = [keyword,'overlap']

    # Correct the timezone difference if we want to show in different timezone than google default (UTC)
    df.index = df.index + timedelta(minutes=tz)

    # Send back only the df of our requested date range
    df = df[start_d:init_end_d]

    # Re-normalized to the overall maximum value to have max =100
    df[keyword] = (100*df[keyword]/df[keyword].max()).round(decimals=0)
    
    return df


if __name__ == "__main__":
    pytrend = TrendReq(hl='en-US')
    keyword = 'iphone'
    start = '2019-01-01'
    end = '2020-10-15'
    geo='US'
    cat=0
    gprop=''

    overlapping = get_daily_trend(pytrend, keyword, start, end, geo=geo, cat=cat, gprop=gprop, verbose=True, tz=0)

