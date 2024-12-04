from dotenv import load_dotenv
import pandas as pd
import requests
from datetime import datetime, timedelta
import os
import time
import json
from typing import List, Dict, Optional

load_dotenv()


# # Search and Import News

# +
class NewsAPICryptoFetcher:
    def __init__(self, api_key):
       
        self.api_key = api_key
        self.base_url = 'https://newsapi.org/v2/everything'
        
        self.headers = {
            "X-Api-Key": self.api_key,
            "User-Agent": "CryptoNewsCollector/1.0"
        }
    
    def fetch_articles(
        self,
        query: str = "bitcoin OR cryptocurrency OR crypto",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        language: str = "en",
        sort_by: str = "publishedAt",
        page_size: int = 100
    ) -> List[Dict]:
        """
        Fetch articles from NewsAPI with pagination handling.
        
        Args:
            query: Search query (default: major crypto terms)
            from_date: Start date in 'YYYY-MM-DD' format
            to_date: End date in 'YYYY-MM-DD' format
            language: Article language (default: English)
            sort_by: Sorting method (relevancy, popularity, publishedAt)
            page_size: Articles per request (max 100)
        """
        all_articles = []
        page = 1
        
        while True:
            params = {
                "q": query,
                "language": language,
                "sortBy": sort_by,
                "pageSize": page_size,
                "page": page
            }
            
            if from_date:
                params["from"] = from_date
            if to_date:
                params["to"] = to_date
                
            try:
                response = requests.get(
                    self.base_url,
                    headers=self.headers,
                    params=params
                )
                response.raise_for_status()
                data = response.json()

                return data
                
                if not data.get("articles"):
                    break
                    
                # Process articles and extract relevant information
                for article in data["articles"]:
                    processed_article = {
                        "title": article.get("title"),
                        "description": article.get("description"),
                        "content": article.get("content"),
                        "published_at": article.get("publishedAt"),
                        "source_name": article.get("source", {}).get("name"),
                        "url": article.get("url")
                    }
                    all_articles.append(processed_article)
                
                # Check if we've reached the end
                if page * page_size >= data["totalResults"]:
                    break
                    
                page += 1
                # Respect rate limits
                time.sleep(0.5)
                
            except requests.exceptions.RequestException as e:
                print(f"Error fetching data: {e}")
                break
                
        return all_articles
    
    def save_articles(self, articles: List[Dict], filename: str):
        """Save articles to CSV or JSON file."""
        if filename.endswith('.csv'):
            df = pd.DataFrame(articles)
            df.to_csv(filename, index=False)
        else:
            with open(filename, 'w') as f:
                json.dump(articles, f, indent=2)
    
    def get_date_batches(self, start_date: str, end_date: str, batch_days: int = 30) -> List[tuple]:
        """
        Split date range into batches to handle API limitations.
        Returns list of (start_date, end_date) tuples.
        """
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        batches = []
        
        current = start
        while current < end:
            batch_end = min(current + timedelta(days=batch_days), end)
            batches.append((
                current.strftime("%Y-%m-%d"),
                batch_end.strftime("%Y-%m-%d")
            ))
            current = batch_end
            
        return batches



# +
API_KEY = os.getenv("NEWS_API_KEY")
fetcher = NewsAPICryptoFetcher(API_KEY)

# Example: Fetch last month's articles
start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
end_date = datetime.now().strftime("%Y-%m-%d")

# Get date batches to handle API limitations
batches = fetcher.get_date_batches(start_date, end_date)

all_articles = []
for batch_start, batch_end in batches:
    print(f"Fetching articles from {batch_start} to {batch_end}")
    articles = fetcher.fetch_articles(
        from_date=batch_start,
        to_date=batch_end,
        query="bitcoin OR cryptocurrency OR crypto"
    )
    all_articles.extend(articles)

# Save to both formats
fetcher.save_articles(all_articles, "crypto_news.csv")
fetcher.save_articles(all_articles, "crypto_news.json")
# -



# # Table Creation

# +
lst = [
    {
        (fld:='source'): article.get(fld, {}).get('domain') or None,
        (fld:='medium'): article.get(fld) or None,
        (fld:='title'): article.get(fld) or None,
        (fld:='summary'): article.get(fld) or None,
        (fld:='sentiment'): article.get(fld) or None
    }
    for article in resp_json.get('articles', [])
]

df = pd.DataFrame(lst)
# -

# # Clean Table

df.fillna('Unknown', inplace=True)
df.drop_duplicates(subset=['title', 'summary'], inplace=True)




