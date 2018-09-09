import datetime

import pandas as pd
import tushare as ts

from lib.token import TOKEN
from setting import split_span

# setting pro api, please use your own token
ts.set_token(TOKEN)
PRO = ts.pro_api()


def date_split(start_date, end_date, span=split_span):
    """
    utility func to split dates into a list of time spans

    Parameters
    ----------
    start_date: str
        start date with format yyyymmdd
    end_date: str
        end date with format yyyymmdd
    span:int
        days of time span, must > 1 or there will be bug

    Returns
    -------
    list of start/end date tuples ("yyyymmdd", "yyyymmdd")
    """
    start_date = datetime.datetime.strptime(start_date, "%Y%m%d")
    end_date = datetime.datetime.strptime(end_date, "%Y%m%d")
    time_spans = []

    while (end_date - start_date).days > span:
        s = datetime.datetime.strftime(start_date, "%Y%m%d")
        start_date = start_date + datetime.timedelta(span)
        e = datetime.datetime.strftime(start_date - datetime.timedelta(1), "%Y%m%d")
        time_spans.append((s, e))

    s = datetime.datetime.strftime(start_date, "%Y%m%d")
    e = datetime.datetime.strftime(end_date, "%Y%m%d")
    time_spans.append((s, e))

    return time_spans


def hist_data_down(stock_id, start_date, end_date):
    """
    utility func to download historical trade data

    Parameters
    ----------
    stock_id: str
        stock id code
    start_date: str
        start date with format yyyymmdd
    end_date: str
        end date with format yyyymmdd

    Returns
    -------
    df of stock trade history data
    """
    time_spans = date_split(start_date, end_date)
    dfs = []

    for start, end in reversed(time_spans):
        try:
            d = PRO.daily(ts_code=stock_id, start_date=start, end_date=end)
            dfs.append(d)
        except:
            raise RuntimeError("{} to {} download error".format(start, end))
    df = pd.concat(dfs, axis=0)

    return df
