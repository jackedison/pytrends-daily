import pandas as pd
import matplotlib
from matplotlib import pyplot as plt
from pull_data import pull_data_to_csv
import yfinance as yf


keyword = 'amazon'
ticker_str = 'AMZN'
method = 'max'
verbose = False
start = '2019-01-01'
end = '2020-10-15'
geo = 'US'
cat = 0
gprop = ''
fetch_new_data = False
compare_daily = True

if fetch_new_data:
    pull_data_to_csv(keyword, start, end, geo, cat, gprop, method, verbose)

df_dly = pd.read_csv('data/{}_daily.csv'.format(keyword), index_col=0, parse_dates=True,)
df_ol = pd.read_csv('data/{}_overlapping.csv'.format(keyword), index_col=0, parse_dates=True,)
df_wkly = pd.read_csv('data/{}_weekly.csv'.format(keyword), index_col=0, parse_dates=True,)

# Set date range of df_oly/wkly same as dly
if compare_daily:
    start_d = df_dly.index[0]
    end_d = df_dly.index[-1]

    df_ol = df_ol.loc[start_d:end_d]
    df_wkly = df_wkly.loc[start_d:end_d]

    # Normalise df_oly/wkly to 100 max
    max_val = df_ol[keyword].max()
    df_ol[keyword] = df_ol[keyword] / max_val * 100

    max_val = df_wkly[keyword].max()
    df_wkly[keyword] = df_wkly[keyword] / max_val * 100

else:
    start_d = df_ol.index[0]
    end_d = df_ol.index[-1]

# Check errors
df = pd.DataFrame({'daily': df_dly[keyword], 'overlap': df_ol[keyword]})
df['mean'] = ((df['daily'] + df['overlap']) / 2)
df['diff'] = df['daily'] - df['overlap']
df['diff'] = df['diff'].abs()
df['diff_pc'] = df['diff'] / df['mean']
df.to_csv('df.csv')

print('Mean value daily: {:.2f}'.format(df['daily'].mean()))
print('Mean value overlap: {:.2f}'.format(df['overlap'].mean()))
print('Mean value overall: {:.2f}'.format(df['mean'].mean()))
print('Average difference: {:.2f}'.format(df['diff'].mean()))
print('Average difference % from mean value overall: {:.2f}%'.format(df['diff_pc'].mean()*100))

# Get stock data
ticker = yf.Ticker(ticker_str)
stock_data = ticker.history(start=start, end=end)
stock_data = stock_data.loc[start_d:end_d]

# Plot the two of them and see if they come out the same
fig, ax = plt.subplots(figsize=(15, 8))
ax2 = ax.twinx()  # 2nd y-axis

plot1 = ax.plot(df_dly.index, df_dly[keyword], label='Standard daily data')
plot2 = ax.plot(df_ol.index, df_ol[keyword], label='Normalised daily data')
plot3 = ax.plot(df_wkly.index, df_wkly[keyword], label='Weekly data')
plot4 = ax2.plot(stock_data.index, stock_data['Close'], label='Price data', color='purple')

ax.set_ylim(0,300)
ax2.set_ylim(0)

ax.set_title('Google search volume for {} - normalised vs standard daily data pull'.format(keyword))
ax.xaxis.set_major_locator(matplotlib.dates.YearLocator())
ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%Y'))

lns = plot1 + plot2 + plot3 + plot4
labs = [l.get_label() for l in lns]
ax.legend(lns, labs, loc='upper left')

plt.savefig(f'models/{keyword}.png')
