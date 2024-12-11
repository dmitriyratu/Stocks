from pathlib import Path
from transformers import pipeline
import torch_directml
from dotenv import load_dotenv
from pandarallel import pandarallel
import pandas as pd
import os
from logger_config import setup_logger
import os



from collect.utils.utils_news_fetcher import CryptoNewsFetcher
from collect.utils.utils_url_scraper import PowerScraper
from collect.utils.utils_news_persist import persist_news


# # Configuration

load_dotenv()
pandarallel.initialize(nb_workers= os.cpu_count() - 1, verbose = 0)


# # Import News URL's


fetcher = CryptoNewsFetcher()

news_df = fetcher.fetch_news(
    pd.Timestamp("2021-01-01"), 
    pd.Timestamp("2021-01-01") + pd.offsets.MonthEnd(0)
)

persist_news(news_df)







news_df.to_parquet(
    '../data/news/crypto_news/', 
    partition_cols=['year_utc', 'month_utc'], 
    existing_data_behavior='append'
)

news_df[news_df['date_month'] == 2]

scraper = PowerScraper()

# +
# %%time
try:
    
    scraper = PowerScraper()
        
    lst = []
    for url in news_df['news_url']:
        print(url)
        lst.append(scraper.scrape(url))
    
    output = pd.DataFrame(lst, columns = ['full_text', 'scrapper','duration'])

finally:
    scraper.close()
# -





output.groupby('scrapper')['duration'].describe()

output.groupby('scrapper')['duration'].sum()







device = torch_directml.device()
summarizer = pipeline("summarization", model="facebook/bart-large-cnn", device=device)        
            # Generate summary based on text length
            word_count = len(TextBlob(article.text).words)
            
            if word_count > 50:
                summary = self.summarizer(
                    article.text,
                    max_length=250,
                    min_length=100,
                    do_sample=False
                )
                article_details["description"] = summary[0]["summary_text"]
            elif word_count > 25:
                article_details["description"] = article.text
            else:
                article_details["description"] = None
                
            return article_details







# # Table Creation

# +
# lst = [
#     {
#         (fld:='source'): article.get(fld, {}).get('domain') or None,
#         (fld:='medium'): article.get(fld) or None,
#         (fld:='title'): article.get(fld) or None,
#         (fld:='summary'): article.get(fld) or None,
#         (fld:='sentiment'): article.get(fld) or None
#     }
#     for article in resp_json.get('articles', [])
# ]

# df = pd.DataFrame(lst)
# -

# # Clean Table

# +
# df.fillna('Unknown', inplace=True)
# df.drop_duplicates(subset=['title', 'summary'], inplace=True)
# -

import torch
print(f"CUDA available: {torch.cuda.is_available()}")
print(f"Device count: {torch.cuda.device_count()}" if torch.cuda.is_available() else "No CUDA devices")
