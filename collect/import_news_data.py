from pathlib import Path
import pandas as pd
from logger_config import setup_logger
from tqdm.notebook import tqdm
import time
import pyprojroot
from deltalake import DeltaTable
from datetime import timedelta

from collect.utils.utils_news_fetcher import CryptoNewsFetcher
from collect.utils.utils_news_persist import persist_news

# # Import News MetaData


fetcher = CryptoNewsFetcher()

base_path = 'data/news/BTC/raw_data/'

dt = DeltaTable(str(pyprojroot.here() / Path(base_path)))
base_table = dt.to_pyarrow_table(columns=['news_id','date_utc']).to_pandas()

news_metadata = fetcher.fetch_news(
    base_table['date_utc'].max() - timedelta(days = 1), 
    pd.Timestamp.now()
)

# ### Persist Data

persist_news(news_metadata, path = base_path)
