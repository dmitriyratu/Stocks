from pathlib import Path
from transformers import pipeline
import torch_directml
from dotenv import load_dotenv
from pandarallel import pandarallel
import pandas as pd
import os
from logger_config import setup_logger
import os
import pyprojroot
import asyncio
from more_itertools import chunked
from tqdm.notebook import tqdm
import numpy as np
from deltalake import DeltaTable

from collect.utils.utils_news_fetcher import CryptoNewsFetcher
from collect.utils.utils_url_scraper import PowerScraper
from collect.utils.utils_news_persist import persist_news

# # Configuration

load_dotenv()


# # Import News MetaData


fetcher = CryptoNewsFetcher()

news_df = fetcher.fetch_news(
    pd.Timestamp("2021-01-02"), 
    pd.Timestamp("2021-01-03") #+ pd.offsets.MonthEnd(0)
)

# # Scrape Article Text

# +
scraper = PowerScraper()

try:

    urls = news_df['news_url'].to_list()
    url_chunks = list(chunked(urls, 20)) 

    total = success = fast_fails = reliable_fails = 0

    with tqdm(total=len(urls)) as pbar:
            
        all_results = []
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

            if n%10 == 0: scraper.refresh_driver_pool()
                
    enrichment_columns = ['news_url', 'full_text', 'full_text_size', 'scrapper', 'scrape_total_seconds']
    scraped_articles = pd.DataFrame(all_results, columns=enrichment_columns)

            
except Exception as e:
    logger.error(exception)
        
finally:
    scraper.close()
# -

enriched_news_df = pd.merge(
    news_df,
    scraped_articles,
    on = ['news_url'],
    how = 'inner'
)

persist_news(enriched_news_df)

enriched_news_df[['selected_text', 'imputed_text_flag']] = enriched_news_df.apply(
    lambda x: (x['preview_text'], True) if pd.isna(x['full_text']) and x['preview_text_size'] > 50 
    else (x['full_text'], False), 
    axis=1
).apply(pd.Series)

clean(
    enriched_news_df['selected_text'].iloc[0],
    extra_spaces = True,
    fix_unicode=True,          # Fix mis-encoded characters
    to_ascii=False,            # Keep non-ASCII characters
    lower=False,               # Keep original casing
    no_line_breaks=False,      # Keep line breaks
    no_urls=True,              # Remove all URLs
    no_emails=True,            # Remove email addresses
    no_phone_numbers=True,     # Remove phone numbers
    no_numbers=False,          # Keep numbers (for financial data)
    no_digits=False,           # Keep digits
    no_currency_symbols=False, # Keep currency symbols
    no_punct=False             # Keep punctuation
)









base_path = pyprojroot.here() / Path('data/news/crypto_news')
test = DeltaTable(str(base_path)).to_pandas()
test







enriched_news_df.groupby('scrapper')['scrape_total_seconds'].describe()

enriched_news_df.groupby('scrapper')['scrape_total_seconds'].sum()

enriched_news_df.loc[enriched_news_df['full_text'].isna(),'title'].iloc[3]

enriched_news_df.apply(
    lambda x: (x['text_preview'],1) if pd.isna(x['full_text']) and x['text_preview_size'] > 50 else (x['full_text'],0), axis = 1
)



enriched_news_df.loc[enriched_news_df['full_text'].isna(),'text_preview'].iloc[5]

enriched_news_df.columns

# +
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
# -





device = torch_directml.device()

summarizer = pipeline("summarization", model="facebook/bart-large-cnn", device=device)        


# # Table Creation


