from dotenv import load_dotenv
import os
import pandas as pd
import numpy as np
import requests
from tqdm.autonotebook import tqdm
import time
from pathlib import Path

from src.core.logging.logger import setup_logger

load_dotenv()

logger = setup_logger("GetCryptoNews", Path("crypto_news.log"))

# # Import News


class CryptoNewsFetcher:

    API_KEY = os.getenv("CRYPTO_NEWS_API_KEY")
    BASE_URL = os.getenv("CRYPTO_NEWS_BASE_URL")
        
    def _post_process_news(self, news):
            
        news_df = pd.DataFrame(news)

        news_df = news_df.rename(columns = {
            'title':'title_text',
            'text':'preview_text',
            'sentiment':'news_api_sentiment',
        })

        news_df = news_df.astype({
            'rank_score':float
        })

        news_df = news_df.drop_duplicates(subset = ['news_url'], ignore_index = True)

        news_df['date_utc'] = pd.to_datetime(news_df['date'], errors='coerce', utc=True).dt.tz_localize(None)

        total_date_nulls = news_df['date_utc'].isna().sum()
        if total_date_nulls > 0:
            logger.warning(f"Date is missing for {total_date_nulls} records, dropping the records.")

        news_df = news_df.dropna(subset = ['date_utc'], ignore_index = True)
        
        news_df['year_utc'] = news_df['date_utc'].dt.year
        news_df['month_utc'] = news_df['date_utc'].dt.month
        news_df['day_utc'] = news_df['date_utc'].dt.day

        news_df['tickers'] = news_df['tickers'].apply(np.asarray)
        news_df['topics'] = news_df['topics'].apply(np.asarray)

        columns = [
            'news_id', 'date', 'date_utc', 'year_utc', 'month_utc', 'day_utc', 'type', 'source_name', 'tickers',
            'topics', 'news_url', 'rank_score', 'news_api_sentiment', 'title_text', 'preview_text'
        ]        

        return news_df[columns]
    
    def fetch_news(self, start_date, end_date):
        params = {
            "token": self.API_KEY,
            "date": "-".join(
                [dt.strftime("%m%d%Y") for dt in [start_date, end_date]]
            ),
            "tickers": "BTC",
            "items": 100,
            "type": "article",
            "sortby": "oldestfirst",
            "extra-fields": "id,rankscore",
            "metadata": 1,
            "fallback": "false",
            "page": 1,
        }

        try:

            logger.info(f"Fetching articles between {start_date.date()} and {end_date.date()}")
            
            news = []
            response = requests.get(self.BASE_URL, params=params).json()

            pages = range(1, response.get("total_pages", 1) + 1)
            for page in tqdm(pages, desc="Fetching news"):
                if page > 1:
                    params["page"] = page
                    response = requests.get(self.BASE_URL, params=params).json()
                data = response.get("data", [])
                if not data:
                    break
                news.extend(data)
                time.sleep(0.5)

            news_df = self._post_process_news(news)

            logger.info(f"Fetched {len(news_df)} out of {len(news)} articles between {start_date.date()} and {end_date.date()}")

            return news_df

        except Exception as e:
            logger.error(f"Error: {str(e)}")
            raise ValueError(e)        
