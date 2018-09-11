import os

import numpy as np
import pandas as pd
from tqdm import tqdm

from lib.collector import stock_list_down, candlestick_chart_down
from setting import retry_count, stock_list_path, log_path, data_path_root


class DataManager:
    def __init__(self, stock_list_path=stock_list_path):
        if not os.path.exists(stock_list_path):
            stock_list_down()
        self.info = pd.read_csv(stock_list_path, dtype=str)
        self.data_df = None
        self.data_dict = None

    def load_data(self, start_date=0):
        """method to load all existing stock data into memory, starting from start_date.
        as self.data_df and self.data_dict. df contains all data in 1 df; dict key is ts_code, value df"""
        assert len(str(start_date)) == 8, "please enter yyyymmdd format date"

        if not os.path.exists(data_path_root):
            print("data path not detected, creating")
            os.mkdir(data_path_root)

        data_list = os.listdir(data_path_root)
        try:
            data_list.remove("stock_list.csv")
        except:
            pass
        if len(data_list) < 1:
            print("no data found! please download with downloader_start method!")
        else:
            data_paths = [os.path.join(data_path_root, i) for i in data_list]
            print("loading data...")
            dfs = []
            self.data_dict = {}
            for i in tqdm(range(len(data_paths))):
                temp = pd.read_csv(data_paths[i])
                temp.drop(temp.loc[temp["trade_date"].astype(np.int) < int(start_date)].index, inplace=True)
                dfs.append(temp)
                self.data_dict[data_paths[i][-13:-4]] = temp
            self.data_df = pd.concat(dfs, axis=0)
            del dfs
            print("{} data loaded!".format(len(data_paths)))

    def downloader_start(self):
        """downloading all stock data to data\, able to update existing files"""
        stock_list_down()
        self.info = pd.read_csv(stock_list_path, dtype=str)

        log = self._down(self.info, self.info.ts_code)

        if len(log) == 0:
            print("download success!")

        else:
            print("there are {} failures, as follows: {}".format(len(log), log))
            print("retrying download")
            log = self._down(self.info, log)

            if len(log) == 0:
                print("download success!")
            else:
                print("there are still {} failures, as follows: {}, saving log to {}".format(len(log), log, log_path))
                err = pd.DataFrame(log, columns=["Failed file"])
                err.to_csv(os.path.join(log_path, "download_error.csv"))

            print("download finished!")

    @staticmethod
    def _down(info, iterator):
        """utility download func for downloader_start method"""
        log = []
        for c in tqdm(iterator):
            flag = "err"
            counter = 0
            while flag == "err":
                flag = candlestick_chart_down(c, info)
                counter += 1
                if (counter > retry_count) and (flag == "err"):
                    log.append(c)
                    break
        return log


if __name__ == "__main__":
    dm = DataManager()
    dm.downloader_start()
    # dm.load_data()
