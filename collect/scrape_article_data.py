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
   ('month_utc', '=', 12),
]
news_metadata = dt.to_pyarrow_table(filters=filters).to_pandas()

# # Scrape Article Text

scraper = PowerScraper()

# +
try:

    urls = pd.unique(news_metadata['news_url']).tolist()
    url_chunks = list(chunked(urls, 100)) 

    total = success = fast_fails = reliable_fails = 0

    all_results = []
    
    with tqdm(total=len(urls)) as pbar:
            
        for n,chunk in enumerate(url_chunks):
            
            results = scraper.scrape_urls(chunk)
            all_results.extend(results)

            for _, content, _, _, _ in results:
                total += 1
                if pd.notna(content):
                    success += 1
                    
                rate = (success / total * 100) if total > 0 else 0
                pbar.set_postfix(success=f"{success}/{total}",rate=f"{rate:.1f}%")
                pbar.update(1)

            scraper.refresh_driver_pool()
                
    enrichment_columns = ['news_url', 'full_text', 'full_text_size', 'scrapper', 'scrape_total_seconds']
    scraped_articles = pd.DataFrame(all_results, columns=enrichment_columns).dropna(subset = ['full_text'], ignore_index = True)
            
except Exception as e:
    logger.error(exception)
        
finally:
    scraper.close()
# -

news_articles = pd.merge(
    news_metadata[['news_id','date_utc','year_utc','month_utc','day_utc','news_url']],
    scraped_articles,
    on = 'news_url',
    how = 'inner'
).reset_index(drop = True)

news_articles.loc[news_articles['full_text_size'].between(0,100),'full_text'].iloc[1]







scraper.scrape_url(news_articles.loc[news_articles['full_text_size'].between(50,100),'news_url'].iloc[0])

news_articles.loc[news_articles['full_text_size'].between(50,100),'full_text'].iloc[0]

news_articles['full_text_size'].describe()

# ### Persist Data

persist_news(news_articles, path = 'data/news/BTC/scraped_data')


