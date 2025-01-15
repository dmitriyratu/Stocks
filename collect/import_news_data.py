from pathlib import Path
import pandas as pd
from logger_config import setup_logger
from tqdm.notebook import tqdm
import time
import pyprojroot
from deltalake import DeltaTable
from datetime import timedelta

from collect.utils.utils_news_fetcher import CryptoNewsFetcher
from collect.utils.utils_news_persist import DeltaLakeManager, TableNames

fetcher = CryptoNewsFetcher()
deltalake = DeltaLakeManager()

# # Ingest News MetaData


base_table = deltalake.read_table(
    table_name = TableNames.METADATA_ARTICLES,
    columns = ['news_id','date_utc']
)

# +
news_metadata = fetcher.fetch_news(
    base_table['date_utc'].max() - timedelta(days = 1), 
    pd.Timestamp.now()
)

news_metadata = news_metadata[~news_metadata['news_id'].isin(base_table['news_id'])]
# -

# # Persist Data

# ## Metadata

deltalake.write_table(
    table_name = TableNames.METADATA_ARTICLES,
    df = news_metadata
)

# ## Status Data

# +
status_table = deltalake.read_table(table_name = TableNames.STATUS_ARTICLES, columns = ['news_id'])

status_table = news_metadata.loc[
    ~news_metadata['news_id'].isin(status_table['news_id']),['news_id']
].assign(**{
    TableNames.METADATA_ARTICLES.value:True,
    TableNames.SCRAPED_ARTICLES.value:False,
    TableNames.CLEANED_ARTICLES.value:False,
    TableNames.LLM_ARTICLES.value:False,
}).reset_index(drop = True)
# -

deltalake.write_table(
    table_name = TableNames.STATUS_ARTICLES,
    df = status_table
)
