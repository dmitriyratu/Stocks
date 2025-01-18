import pandas as pd
from pathlib import Path
from more_itertools import chunked
from tqdm.notebook import tqdm
import pyprojroot
from deltalake import DeltaTable


from src.core.storage.delta_lake import DeltaLakeManager, TableNames
from src.collect.utils.article_scraper import PowerScraper
from src.core.logging.logger import setup_logger

deltalake = DeltaLakeManager()

# # Import Data

# ## Import Status Data

status_table = deltalake.read_table(
    table_name = TableNames.STATUS_ARTICLES, 
    filters = [
        (TableNames.METADATA_ARTICLES.value, "=", True),
        (TableNames.SCRAPED_ARTICLES.value, "=", False),
    ]
)

news_id_list = status_table['news_id'].tolist()

# ## Import News MetaData

news_metadata = deltalake.read_table(table_name = TableNames.METADATA_ARTICLES, filters = [("news_id", "in", news_id_list)])

# # Scrape Article Text

# +
urls = pd.unique(news_metadata['news_url']).tolist()
url_chunks = list(chunked(urls, 100))
all_results = []
total_success = 0

with tqdm(total=len(urls)) as pbar:
    for chunk in url_chunks:
        with PowerScraper() as scraper:
            results = scraper.scrape_urls(chunk)
            all_results.extend(results)

            chunk_success = sum(1 for r in results if r.success)
            total_success += chunk_success
            
            pbar.update(len(chunk))
            pbar.set_postfix(
                chunk=f"{chunk_success}/{len(chunk)}",
                total=f"{total_success}/{len(all_results)} ({total_success/len(all_results)*100:.1f}%)"
            )
# -
# ## Merge Data

news_articles = pd.merge(
    news_metadata[['news_id', 'news_url','date_utc','year_utc','month_utc','day_utc']],
    pd.DataFrame(all_results), 
    how = 'left'
)

# # Persist Data

# ## Scraped Data

deltalake.write_table(
    table_name = TableNames.SCRAPED_ARTICLES,
    df = news_articles
)

# ## Status Data

status_table.loc[status_table['news_id'].isin(news_id_list), TableNames.SCRAPED_ARTICLES.value] = True
deltalake.write_table(
    table_name = TableNames.STATUS_ARTICLES,
    df = status_table
)
