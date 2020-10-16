import pandas as pd
import matplotlib
from matplotlib import pyplot as plt
from pull_data import pull_data_to_csv

keyword = 'amazon'

pull_data_to_csv(keyword)

df_dly = pd.read_csv('{}_daily.csv'.format(keyword), index_col=0)
df_ol = pd.read_csv('{}_overlapping.csv'.format(keyword), index_col=0)

# Set date range of df_dly same as ol
start_d = df_dly.index[0]
end_d = df_dly.index[-1]

df_ol = df_ol.loc[start_d:end_d]

# Normalise df_dly to 100 max
max_val = df_ol[keyword].max()
df_ol[keyword] = df_ol[keyword] / max_val * 100

# Plot the two of them and see if they come out the same
fig = plt.figure()
ax = fig.gca()
plot1 = ax.plot(df_dly.index, df_dly[keyword], label='Standard daily data')
plot2 = ax.plot(df_ol.index, df_ol[keyword], label='Normalised daily data')

plt.title('Google search volume for {} - normalised vs standard daily data pull'.format(keyword))
ax.xaxis.set_major_locator(matplotlib.dates.MonthLocator())
ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%b'))
plt.locator_params(axis='x', nbins=10)

ax.legend()

plt.show()


# Slightly off - 10%? Timezone?
