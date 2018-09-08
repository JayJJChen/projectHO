# stream line A share data downloader

import datetime
import os

import pandas as pd
import tushare as ts
from tqdm import tqdm

from setting import data_path_root, stock_list_path
from lib.token import TOKEN
from lib.util import hist_data_down

# setting pro api, please use your own token
ts.set_token(TOKEN)
PRO = ts.pro_api()


def stock_list_down(path=stock_list_path):
    """downloading up-to-date stock list & basic info"""
    print("downloading stock list, path:", path)
    df = PRO.stock_basic()
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
        start date in yyyymmdd format
    end_date: str
        end date in yyyymmdd format

    Returns
    -------

    """
    try:
        csv_path = os.path.join(data_path_root, stock_id + ".csv")
        exist = os.path.exists(csv_path)

        if info is None:
            info = pd.read_csv(stock_list_path, dtype=str)

        if exist:
            df = pd.read_csv(csv_path, dtype=str)

        # getting start/end date, no sanity check
        if start_date == "latest":
            if not exist:
                print("stock_id {} not found in local storage, downloading from very beginning".format(stock_id))
                start_date = str(info.loc[info.ts_code == stock_id, 'list_date'].values[0])
            else:
                start_date = (datetime.datetime.strptime(df.trade_date[0], "%Y%m%d")
                              + datetime.timedelta(1)).strftime("%Y%m%d")

        if type(end_date) != str:
            end_date = end_date.strftime("%Y%m%d")

        if exist:
            df_new = hist_data_down(stock_id, start_date=start_date, end_date=end_date)
            df_sum = pd.concat([df_new, df], axis=0)
        else:
            df_sum = hist_data_down(stock_id, start_date=start_date, end_date=end_date)

        if len(df_sum) > 0:
            df_sum.to_csv(csv_path, index=None)

        return "success"

    except:
        print("{} saving failed".format(stock_id))
        return "err"


if __name__ == "__main__":
    stock_list_down()
    info = pd.read_csv(stock_list_path, dtype=str)
    for c in tqdm(info.ts_code):
        print("downloading", c)
        candlestick_chart_down(c, info=info)
