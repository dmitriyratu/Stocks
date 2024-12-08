# +
import os
import time
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from logger_config import setup_logger
from newspaper import Article, Config
from textblob import TextBlob
from tqdm.autonotebook import tqdm
from transformers import pipeline
import torch_directml
from functools import lru_cache
import requests
from newspaper import Article, Config
from time import sleep
import random
from typing import Optional, Dict, Union
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from fake_useragent import UserAgent
from fp.fp import FreeProxy
import random
# -

# # Configuration

load_dotenv()


log_file = Path("../logs/crypto_news.log")
logger = setup_logger("GetCryptoNews", log_file)

# # Import News URL's


class CryptoNewsFetcher:
    def __init__(self):
        self.api_key = os.getenv("CRYPTO_NEWS_API_KEY")
        self.base_url = os.getenv("CRYPTO_NEWS_BASE_URL")
        
        

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
            "extra-fields": "id,eventid,rankscore",
            "metadata": 1,
            "fallback": "false",
            "page": 1,
        }

        try:
            all_news = []
            response = requests.get(self.base_url, params=params).json()

            pages = range(1, response.get("total_pages", 1) + 1)
            for page in tqdm(pages, desc="Fetching news"):
                if page > 1:
                    params["page"] = page
                    response = requests.get(self.base_url, params=params).json()
                data = response.get("data", [])
                if not data:
                    break
                all_news.extend(data)
                time.sleep(0.5)

            logger.info(f"Fetched {len(all_news)} articles")
            return all_news

        except Exception as e:
            logger.error(f"Error: {str(e)}")
            return []

fetcher = CryptoNewsFetcher()

news = fetcher.fetch_news("2021-01-01", "2021-01-02")


# +
# Example result structure:
# {
#     "kind": "news",
#     "domain": "coindesk.com",
#     "source": {"title": "CoinDesk", "region": "en"},
#     "title": "Bitcoin Surges Past $40K",
#     "published_at": "2023-12-04T10:30:00Z",
#     "url": "https://www.coindesk.com/article-url..."
# }
# -






class ProxyManager:
    """Manages a pool of free proxies."""
    
    def __init__(self):
        """
        Initialize proxy manager with configuration.
        
        Args:
            min_pool_size: Minimum number of proxies to maintain in the pool
            max_attempts: Maximum attempts to fetch new proxies
            timeout: Timeout for proxy validation in seconds
        """
        self.min_pool_size = 5
        self.max_attempts = 10
        self.timeout = 1
        self.proxy_pool: Set[str] = set()
        self.refresh_pool()

    def refresh_pool(self) -> None:
        """Refresh the pool of proxies."""
        for _ in range(self.max_attempts):
            if len(self.proxy_pool) >= self.min_pool_size:
                break
            try:
                if proxy := FreeProxy(
                    timeout=self.timeout,
                    rand=True,
                    anonym=True
                ).get():
                    self.proxy_pool.add(proxy)
            except Exception as e:
                logger.warning(f"Failed to fetch proxy: {e}")
            sleep(1)

    def get_proxy(self) -> Optional[Dict[str, str]]:
        """Get a random proxy from the pool."""
        if len(self.proxy_pool) < self.min_pool_size // 2:
            self.refresh_pool()
        
        if not self.proxy_pool:
            return None
        
        proxy = random.choice(tuple(self.proxy_pool))
        return {'http': proxy, 'https': proxy}

    def remove_proxy(self, proxy: Dict[str, str]) -> None:
        """Remove a failed proxy from the pool."""
        for protocol in ('http', 'https'):
            self.proxy_pool.discard(proxy.get(protocol, ''))


class ArticleScraper:
    """Article scraper with proxy rotation and retry mechanisms."""
    
    def __init__(self, max_retries: int = 3):
        self.max_retries = max_retries
        self.session = self._create_session()
        self.proxy_manager = ProxyManager()
        self.ua = UserAgent()

    def _create_session(self) -> requests.Session:
        """Create a session with retry strategy."""
        session = requests.Session()
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _get_headers(self) -> Dict[str, str]:
        """Generate headers with rotating user agent."""
        return {
            "User-Agent": self.ua.random,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Referer": "https://www.google.com",
            "Connection": "keep-alive"
        }

    def get_article(self, url: str, retry_count: int = 0) -> Union[str, Dict[str, str]]:
        """Get article content with automatic retries and proxy rotation."""
        
        if retry_count >= self.max_retries:
            raise ValueError(f"Maximum retry attempts reached; url: {url}")

        try:
            sleep(random.uniform(1, 3))
            response = self.session.get(
                url,
                headers=self._get_headers(),
                proxies=self.proxy_manager.get_proxy(),
                timeout=10,
                verify=False
            )
            response.raise_for_status()
            
            article = Article(url)
            article.set_html(response.text)
            article.parse()
            
            if not article.text:
                raise ValueError("No article text found")
            
            return article.text

        except Exception as e:
            logger.warning(f"Attempt {retry_count + 1} failed: {str(error)}")
            sleep(2 ** retry_count)
            return self.get_article(url, retry_count + 1)

    def close(self) -> None:
        """Close resources."""
        self.session.close()


scraper = ArticleScraper()

content = scraper.get_article(news[0]['news_url'])
display(news[0]['news_url'])

scraper.close()





# +

# Example usage:
# scraper = FreeArticleScraper(use_proxies=False, max_retries=3)
# content = scraper.get_article("https://example.com/article")
# -





scraper = FreeArticleScraper()
article_data = scraper.get_article(news[10]['news_url'])
article_data

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







# +

class FreeArticleScraper:
    """A free solution to scrape news articles."""
    
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Referer": "https://www.google.com",
        }

    def get_article(self, url):
        try:
            
            # Fetch page content using requests
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status() 
            
            # Parse the article with newspaper3k
            article = Article(url)
            article.set_html(response.text)
            article.parse()

            return article.text
            
        except Exception as e:
            return {"error": f"Failed to scrape article: {e}", "url": url}


# -

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
