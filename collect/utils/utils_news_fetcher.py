from dotenv import load_dotenv
import os
import pandas as pd
import requests
from tqdm.autonotebook import tqdm
import time
from logger_config import setup_logger
from pathlib import Path
import pyprojroot

load_dotenv()

log_file = pyprojroot.here() / Path("logs/crypto_news.log")
logger = setup_logger("GetCryptoNews", log_file)

# # Import News


class CryptoNewsFetcher:
    def __init__(self):
        
        self.api_key = os.getenv("CRYPTO_NEWS_API_KEY")
        self.base_url = os.getenv("CRYPTO_NEWS_BASE_URL")

    def _post_process_news(self,news):
            
        news_df = pd.DataFrame(news)

        news_df['date_utc'] = pd.to_datetime(news_df['date']).dt.tz_convert('UTC').dt.tz_localize(None)
        
        news_df['year_utc'] = news_df['date_utc'].dt.year
        news_df['month_utc'] = news_df['date_utc'].dt.month
        news_df['day_utc'] = news_df['date_utc'].dt.day


        news_df.rename(columns = {
            'text':'text_preview',
            'sentiment':'news_api_sentiment',
        }, inplace = True)

        news_df = news_df.astype({
            'rank_score':float
        })

        columns = [
            'news_id', 'date', 'date_utc', 'year_utc', 'month_utc', 'day_utc', 'type', 'source_name', 'tickers',
            'topics', 'news_url', 'rank_score', 'news_api_sentiment', 'title', 'text_preview'
        ]        

        return news_df[columns]
    
    def fetch_news(self, start_date, end_date):
        params = {
            "token": self.api_key,
            "date": "-".join(
                [pd.Timestamp(dt).strftime("%m%d%Y") for dt in [start_date, end_date]]
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
            news = []
            response = requests.get(self.base_url, params=params).json()

            pages = range(1, response.get("total_pages", 1) + 1)
            for page in tqdm(pages, desc="Fetching news"):
                if page > 1:
                    params["page"] = page
                    response = requests.get(self.base_url, params=params).json()
                data = response.get("data", [])
                if not data:
                    break
                news.extend(data)
                time.sleep(0.5)

            logger.info(f"Fetched {len(news)} articles")

            news_df = self._post_process_news(news)

            return news_df

        except Exception as e:
            logger.error(f"Error: {str(e)}")
            raise ValueError(e)        
