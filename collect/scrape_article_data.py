import pandas as pd
from pathlib import Path
from more_itertools import chunked
from tqdm.notebook import tqdm
from logger_config import setup_logger
import pyprojroot
from deltalake import DeltaTable


from collect.utils.utils_url_scraper import PowerScraper
from collect.utils.utils_news_persist import persist_news

# # Import News Data

base_path = pyprojroot.here() / Path('data/news/BTC/raw_data/')
dt = DeltaTable(str(base_path))
filters = [
    ('year_utc', '=', 2024),
    ('month_utc', '=', 3),
]
news_metadata = dt.to_pyarrow_table(filters=filters).to_pandas()

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
# ### Merge Data

news_articles = pd.merge(
    news_metadata[['news_id', 'news_url','date_utc','year_utc','month_utc','day_utc']],
    pd.DataFrame(all_results), 
    how = 'left'
)

# ### Persist Data

persist_news(news_articles, path = 'data/news/BTC/scraped_data')
