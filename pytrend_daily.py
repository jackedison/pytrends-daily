import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
from pytrends.request import TrendReq
from pytrends.exceptions import ResponseError


def fetch_data(trendreq, kw_list, timeframe='today 3-m', cat=0, geo='',
               gprop='') -> pd.DataFrame:

    """Pull google trends data using pytrends package"""
    attempts, fetched = 0, False
    while not fetched:
        try:
            trendreq.build_payload(kw_list=kw_list, timeframe=timeframe,
                                   cat=cat, geo=geo, gprop=gprop)
        except ResponseError as err:
            print(err)
            print(f'Trying new pull in {60 + 5 * attempts} seconds.')
            time.sleep(60 + 5 * attempts)
            attempts += 1
            if attempts > 3:
                print('Failed after 3 attemps, aborting.')
                break
        else:
            fetched = True
    return trendreq.interest_over_time()


def get_coefficient(df1, df2, start, end, method='mean'):

    """Get coefficient to adjust data by. Refactored for testing/optimising"""

    if method == 'max':
        # Normalise using the maximum value of the overlapped period
        # Y1 is max data value in our new data in the 100 day overlap period
        y1 = df1.loc[start:end].iloc[:,-1].values.max()

        # Y2 is the max data in our existing data in the 100 day overlap period
        y2 = df2.loc[start:end].iloc[:,-1].values.max()

        coef = y2/y1

    elif method == 'min':
        y1 = df1.loc[start:end].iloc[:,-1].values.min()

        y2 = df2.loc[start:end].iloc[:,-1].values.min()

        coef = y2/y1

    elif method == 'mean':
        # Normalise using the mean ratio
        ratios = df2.loc[start:end].iloc[:,-1].values / df1.loc[start:end].iloc[:,-1].values

        coef = ratios.mean()

    elif method == 'sum':
        # Normalise using the sum of all values ratio
        y1 = df1.loc[start:end].iloc[:,-1].values.sum()

        y2 = df2.loc[start:end].iloc[:,-1].values.sum()

        coef = y2/y1

    else:
        raise ValueError(f'Method of time {method} does not exist')
    return coef


def get_daily_trend(trendreq, keyword:str, start:str, end:str, cat=0, 
                    geo='', gprop='', delta=269, overlap=100, sleep=0, 
                    verbose=False, method='max') ->pd.DataFrame:

    """Pull daily google trend data for longer date range (>269) and normalise

        Parameters
        ----------
        trendreq : TrendReq
            a pytrends TrendReq object
        keyword: str
            supports 1 keyword
        start: str
            starting date in string format:YYYY-MM-DD (e.g.2017-02-19)
        end: str
            ending date in string format:YYYY-MM-DD (e.g.2017-02-19)
        cat, geo, gprop, sleep: 
            same as defined in pytrends
        delta: int
            The length (days) of each timeframe fragment.
            Must be <269 in order to obtain daily data.
        overlap: int
            The length (days) of the overlap period used for normalisation.
            100 default.
        method: str
            max, mean, sum: overlapping normalising method
    """
    
    # Set start dates, end dates, and date period jump in queryable formats
    start_d = datetime.strptime(start, '%Y-%m-%d')
    init_end_d = end_d = datetime.strptime(end, '%Y-%m-%d')
    init_end_d.replace(hour=23, minute=59, second=59)    
    delta = timedelta(days=delta)  # default delta 269 days
    overlap = timedelta(days=overlap)  # default overlap of 100 days

    itr_d = end_d - delta
    overlap_start = None

    df = pd.DataFrame()  # To store google interest data
    ol = pd.DataFrame()  # To store which dates were overlapped
    
    while end_d > start_d:
        # Call google for data of our current data range
        tf = itr_d.strftime('%Y-%m-%d')+' '+end_d.strftime('%Y-%m-%d')
        if verbose:
            print('Fetching \''+keyword+'\' for period:'+tf)
        temp = fetch_data(trendreq, [keyword], timeframe=tf, cat=cat, geo=geo, gprop=gprop)

        # Fix the data by removing isPartial column and call data col the date range
        temp.drop(columns=['isPartial'], inplace=True)
        temp.columns.values[0] = tf

        # Create a copy of the data and remove all data values. So skeleton db of dates
        ol_temp = temp.copy()
        ol_temp.iloc[:,:] = None

        # If we have overlapping data (i.e. if not first pull):
        if overlap_start is not None:
            if verbose:
                print('Normalise by overlapping period:'+overlap_start.strftime('%Y-%m-%d'), end_d.strftime('%Y-%m-%d'))

            # Get coefficient to adjust new data by
            coef = get_coefficient(df1=temp, df2=df,
                                   start=overlap_start, end=end_d,
                                   method=method)
            
            # Adjust new data
            temp = temp * coef

            # To our skeleton date df set =1 in a column headed with the overlap period (Empty if not)
            ol_temp.loc[overlap_start:end_d, :] = 1 

        # Concatenate the main df and overlap df with our new data
        # Note: this will add an extra column as col header is date range
        df = pd.concat([df,temp], axis=1)
        ol = pd.concat([ol, ol_temp], axis=1)

        # Shift the timeframe for next iteration
        overlap_start = itr_d
        end_d -= (delta-overlap)
        itr_d -= (delta-overlap)

        # If server is limiting access then sleep parameter can be used
        time.sleep(sleep)
    
    # Once loop is completed we will have:
    # 1. df: has all data normalised based on max values over ol period
    # 2. ol: has all dates we fetched data for with a 1 if it was overlapped
    df.sort_index(inplace=True)
    ol.sort_index(inplace=True)


    # If we are fetching data until today, the google daily trend data will not yet have updated with the most recent 3 days of data
    # So we will need to fetch it with hourly data and normalise
    if df.index.max() < init_end_d : 
        # Get data for most recent 7 days with the 'now 7-d' timeframe parameter. 4 days to normalise, 3 days new data
        tf = 'now 7-d'
        hourly = fetch_data(trendreq, [keyword], timeframe=tf, cat=cat, geo=geo, gprop=gprop)
        hourly.drop(columns=['isPartial'], inplace=True)
        
        # Convert hourly data to daily data by summing every hour into a day row
        daily = hourly.groupby(hourly.index.date).sum()
        
        # d-7 usually won't have full 24h with 'now 7-d' pull. So just remove it
        daily['hours'] = hourly.groupby(hourly.index.date).count()
        if daily.iloc[0].loc['hours'] != 24:
            daily.drop(daily.index[0], inplace=True)
        daily.drop(columns='hours', inplace=True)
        
        # Set index to the dates and the column header to 'now 7-d'
        daily.set_index(pd.DatetimeIndex(daily.index), inplace=True)
        daily.columns = [tf]

        # Copy the dates to ol_temp and remove the data. Skeleton copy for ol    
        ol_temp = daily.copy()
        ol_temp.iloc[:,:] = None

        # Find the overlapping dates
        intersect = df.index.intersection(daily.index)
        if verbose:
            print('Normalise by overlapping period:'+(intersect.min().strftime('%Y-%m-%d'))+' '+(intersect.max().strftime('%Y-%m-%d')))

        # Normalise the new data using the overlapped period of today-4 to today-7.
        coef = get_coefficient(df1=daily, df2=df,
                               start=intersect.min(), end=intersect.max(),
                               method=method)
        daily = (daily*coef).round(decimals=0)
        ol_temp.loc[intersect,:] = 1
        
        # Add the final 3 days to our data
        df = pd.concat([daily, df], axis=1)
        ol = pd.concat([ol_temp, ol], axis=1)

    # For our overlapped periods, we will take the mean of our the pulled data, and our adjusted pulled data with the coefficients
    if method == 'max' or 'min':
        df = df.mean(axis=1)
    elif method == 'mean' or 'sum':
        # Create new column
        # For each column if has value yes, if not no

        # Check for col is not nan. First will be used if multiple so reverse
        conditions = [(~df[column].isna()) for column in df][::-1]

        # List of arrays to check choices on
        choices = [df[column] for column in df][::-1]

        df[keyword] = np.select(conditions, choices, default=df[df.columns[0]])

        # Drop all cols except keyword
        df = df[[keyword]]
    
    ol = ol.max(axis=1)

    # Then merge the two dataframe (trend data and flag of whether it was overlapped)
    df = pd.concat([df,ol], axis=1)
    df.columns = [keyword,'overlap']

    # Send back only the df of our requested date range
    df = df[start_d:init_end_d]

    # Re-normalized to the overall maximum value of 100 search interest
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

    overlapping = get_daily_trend(pytrend, keyword, start, end, geo=geo, cat=cat, gprop=gprop, verbose=True)

