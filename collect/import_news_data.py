from pathlib import Path
import pandas as pd
from logger_config import setup_logger
from tqdm.notebook import tqdm
import time

from collect.utils.utils_news_fetcher import CryptoNewsFetcher
from collect.utils.utils_news_persist import persist_news

# # Import News MetaData


fetcher = CryptoNewsFetcher()

today = pd.Timestamp.now()

dt_ranges = pd.date_range(start = pd.Timestamp('2023-05-01 00:00:00'), end = today, freq = 'MS')
for dt in tqdm(dt_ranges):
    news_metadata = fetcher.fetch_news(
        dt, dt + pd.offsets.MonthEnd(0)
    )
    time.sleep(10) 

# ### Persist Data

persist_news(news_metadata, path = 'data/news/BTC/raw_data')
