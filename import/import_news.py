# +
import os
from transformers import pipeline
from textblob import TextBlob
from newspaper import Article
from dotenv import load_dotenv
import requests
from datetime import datetime, timedelta, timezone
import time
import json
import pandas as pd
from pathlib import Path
from tqdm.autonotebook import tqdm

from logger_config import setup_logger
# -

load_dotenv()


# # Search and Import News

class CryptoNewsFetcher:
    def __init__(self):
        self.api_key = os.getenv('CRYPTO_NEWS_API_KEY')
        self.base_url = os.getenv('CRYPTO_NEWS_BASE_URL')
        log_file = Path("../logs/crypto_news.log")
        self.logger = setup_logger("GetCryptoNews", log_file)

    def fetch_news(self, start_date, end_date):
        params = {
            "token": self.api_key,
            "date": '-'.join([pd.Timestamp(dt).strftime('%m%d%Y') for dt in [start_date, end_date]]),
            "tickers": "BTC",
            'items': 100,
            "type": "article",
            "sortby": "oldestfirst",
            "extra-fields": "id,eventid,rankscore",
            "metadata": 1,
            "fallback": "false",
            "page": 1,
        }
        
        try:
            all_news = []
            full_request = requests.get(self.base_url, params=params).json()
            
            pages = range(1, full_request.get("total_pages", 1) + 1)
            for page in tqdm(pages, desc="Fetching news"):
                if page > 1:
                    params['page'] = page
                    full_request = requests.get(self.base_url, params=params).json()
                data = full_request.get("data", [])
                if not data:
                    break
                all_news.extend(data)
                time.sleep(0.5)

            self.logger.info(f"Fetched {len(all_news)} articles")
            return all_news

        except Exception as e:
            self.logger.error(f"Error: {str(e)}")
            return []


fetcher = CryptoNewsFetcher()

news = fetcher.fetch_news("2021-01-01", "2021-01-04")





response.json().keys()


def _get_total_pages(self, params):
    
    if response.status_code != 200:
        self.logger.error(f"Failed to get total pages: {response.status_code}, {response.text}")
        return 0
    
    total_items = response.json().get("total", 0)
    return min(100, -(-total_items // params['items']))


'trending-headlines?'





# +

# Print number of articles and some details
print(f"Found {len(news)} articles from {start_date} to {end_date}.")
for article in news[:5]:  # Print details for the first 5 articles
    print(f"- {article['title']} (Published: {article['published_at']})")

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









# +
summarizer = pipeline("summarization", model="facebook/bart-large-cnn", device=0)

def extract_article_details(url):
    """
    Extract details from a news article URL and always generate a summary using transformers.
    
    Args:
        url (str): URL of the news article.
        max_length (int): Maximum length of the transformer-generated summary.
        min_length (int): Minimum length of the transformer-generated summary.

    Returns:
        dict: Dictionary containing article details (text, title, author, publish date, and summary).
    """
    
    # Initialize the Article object
    article = Article(url)

    # Download and parse the article
    article.download()
    article.parse()
    
    # Extract details
    article_details = {
        "title": article.title,
        "text": article.text,
        "authors": article.authors,
        "publish_date": article.publish_date,
        "url": url
    }

    # Generate a summary using transformer-based summarization
    if len(TextBlob(article.text).words) > 50:
        transformer_summary = summarizer(
            article.text,
            max_length=250,
            min_length=100,
            do_sample=False
        )
        article_details["description"] = transformer_summary[0]['summary_text']
    elif len(TextBlob(article.text).words) > 25:
        article_details["description"] = article.text
    else:
        article_details["description"] = None
        
    return article_details


# +
# Example usage
example_url = 'https://decrypt.co/52971/bitcoin-exceeds-30k-to-hit-yes-another-all-time-high'
article_details = extract_article_details(example_url)


# -
article_details

# Display results
if "error" in article_details:
    print(f"Failed to extract article: {article_details['error']}")
else:
    print(f"Title: {article_details['title']}")
    print(f"Authors: {article_details['authors']}")
    print(f"Publish Date: {article_details['publish_date']}")
    print(f"Summary: {article_details.get('description', 'No summary available')}")


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


