import os

import pandas as pd
from tqdm import tqdm

from lib.collector import stock_list_down, candlestick_chart_down
from setting import retry_count, stock_list_path, log_path


class DataManager:
    def __init__(self, stock_list_path=stock_list_path):
        if not os.path.exists(stock_list_path):
            stock_list_down()
        self.info = pd.read_csv(stock_list_path, dtype=str)

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

    def downloader_start(self):
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


if __name__ == "__main__":
    dm = DataManager()
    dm.downloader_start()
