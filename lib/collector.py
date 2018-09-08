# stream line A share data downloader

import datetime
import os
import time

import pandas as pd
import tushare as ts

from lib.util import six_digit_date_split
from setting import data_path_root, stock_list_path


def stock_list_down(path=stock_list_path):
    """downloading up-to-date stock list & basic info"""
    print("downloading stock list, path:", path)
    df = ts.get_stock_basics()
    df.to_csv(path)
    print("download finished")
    return None


def candlestick_chart_down(stock_id, info=None, start_date="latest", end_date=datetime.date.today()):
    """
    func to download candlestick charts and save

    Parameters
    ----------
    stock_id: str
        stock id code
    info: DataFrame
        the data frame generated by stock_list_down
    start_date: str
        start date in yyyy-mm-dd format
    end_date: str
        end date in yyyy-mm-dd format

    Returns
    -------

    """
    try:
        csv_path = os.path.join(data_path_root, stock_id + ".csv")
        exist = os.path.exists(csv_path)

        if info is None:
            info = pd.read_csv(stock_list_path, dtype=str)

        if exist:
            df = pd.read_csv(csv_path)

        # getting start/end date, no sanity check
        if start_date == "latest":
            if not exist:
                print("stock_id {} not found in local storage, downloading from very beginning".format(stock_id))
                start_date = six_digit_date_split(info.loc[info.code == stock_id, 'timeToMarket'].values[0])
            else:
                start_date = (datetime.datetime.strptime(df.date[0], "%Y-%m-%d")
                              + datetime.timedelta(1)).strftime("%Y-%m-%d")

        if type(end_date) != str:
            end_date = end_date.strftime("%Y-%m-%d")

        if exist:
            # df_new = hist_data_down(stock_id, start_date, end_date)
            # df_sum = pd.concat([df_new, df], axis=0)
            df_new = ts.get_h_data(stock_id, start=start_date, end=end_date)
            df_sum = pd.concat([df_new, df], axis=0)
        else:
            # df_sum = hist_data_down(stock_id, start_date, end_date)
            df_sum = ts.get_h_data(stock_id, start=start_date, end=end_date)

        if len(df_sum) > 0:
            df_sum.to_csv(csv_path)

        return "success"

    except:
        print("{} saving failed".format(stock_id))
        time.sleep(300)
        return "err"


if __name__ == "__main__":
    # stock_list_down()
    info = pd.read_csv(stock_list_path, dtype=str)
    for c in info.code:
        if int(info.loc[info.code == c, "timeToMarket"].values[0]) > 20050101:
            continue
        r = "err"
        while r == "err":
            r = candlestick_chart_down(c, info=info, start_date="2005-01-01", end_date="2008-01-01")
    # for c in info.code:
    #     candlestick_chart_down(stock_id, info=info, end_date="2015-01-01")
    # for c in info.code:
    #     candlestick_chart_down(stock_id, info=info)
