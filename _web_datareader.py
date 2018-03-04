"""
eod_datareader._web_datareader
~~~~~~~~
This module implements methods for scraping pandas_datareader.


:author: leoliu49
"""
import pandas
from pandas_datareader import data as web


def query(company, dt_start, dt_end, source):
    dataframe = web.DataReader(company, source, dt_start, dt_end)
    return dataframe
