import os

data_path_root = "E:\Projects\projectHO\data"
stock_list_path = os.path.join(data_path_root, "stock_list.csv")
split_span = 365 * 5  # split dates to download pre-historical data
retry_count = 5  # downloader max retry count
log_path = "E:\Projects\projectHO\log"
