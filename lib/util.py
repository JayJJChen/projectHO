import datetime

import pandas as pd
import tushare as ts

from setting import split_span


def six_digit_date_split(six_digit):
    """
    utility func to convert six digit date in stock_list.csv into yyyy-mm-dd format string
    Parameters
    ----------
    six_digit: int
        date to be converted

    Returns
    -------
    yyyy-mm-dd format string
    """
    six_digit = str(six_digit)
    six_digit = six_digit[:4] + "-" + six_digit[4:6] + "-" + six_digit[6:]
    return six_digit


def date_split(start_date, end_date, span=split_span):
    """
    utility func to split dates into a list of time spans

    Parameters
    ----------
    start_date: str
        start date with format yyyy-mm-dd
    end_date: str
        end date with format yyyy-mm-dd
    span:int
        days of time span, must > 1 or there will be bug

    Returns
    -------
    list of start/end date tuples ("yyyy-mm-dd", "yyyy-mm-dd")
    """
    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    time_spans = []

    while (end_date - start_date).days > span:
        s = datetime.datetime.strftime(start_date, "%Y-%m-%d")
        start_date = start_date + datetime.timedelta(span)
        e = datetime.datetime.strftime(start_date - datetime.timedelta(1), "%Y-%m-%d")
        time_spans.append((s, e))

    s = datetime.datetime.strftime(start_date, "%Y-%m-%d")
    e = datetime.datetime.strftime(end_date, "%Y-%m-%d")
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
        start date with format yyyy-mm-dd
    end_date: str
        end date with format yyyy-mm-dd

    Returns
    -------
    df of stock trade history data
    """
    time_spans = date_split(start_date, end_date)
    dfs = []

    for start, end in reversed(time_spans):
        try:
            dfs.append(ts.get_h_data(stock_id, start=start, end=end, pause=1))
        except:
            print("{} to {} download error".format(start, end))

    df = pd.concat(dfs, axis=0)

    return df

# if __name__ == "__main__":
#     print(date_split("2012-01-02", "2012-01-05", 1))
#     df = hist_data_down("600419", "2010-10-10", "2018-02-02")
#     df.to_csv("exp.csv")
