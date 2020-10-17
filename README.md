<h1>Pytrends-daily</h1>

This package began as a fork of [gtrends](https://github.com/ecsalina/gtrends) which adds [normalised, daily data pulls](https://towardsdatascience.com/reconstruct-google-trends-daily-data-for-extended-period-75b6ca1d3420) to the [pytrends](https://github.com/GeneralMills/pytrends) API for google trends.

It now includes:

* Daily google trend data pull
* Weekly google trend data pull
* Daily overlapping google trend data pull (for >270 days). This can use max, min, mean, or mean of sum to normalise new daily data.
* Yahoo finance data pull for stocks/commodities/currencies
* Standard modelling. Feel free to add more advanced with data sources.

Requires pandas, numpy, matplotlib, pytrends, yfinance.

**Note: there is a slight discrepency in the overlapping & normalising method vs standard data pull. Using a max/mean, min/mean, aggregate mean, and mean of sum ratio to adjust overlapping have all been tried and all have this error. Results in a difference of 1.3% - 15% depending on the query. ('amazon' vs 'buy gold').**

**Perhaps there is random noise that google adds? Or some integer rounding - but confusing as to why one query is 10x more variable than another.**

Possible extensions
* fix overlapping error if error source is determined & fixable (i.e. not random noise/integer rounding)
* enable keyword search by unique identifier (e.g. apple by fruit or by company)
* more advanced modelling and trend analysis
