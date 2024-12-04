import os
from transformers import pipeline
from textblob import TextBlob
from newspaper import Article


load_dotenv()

API_KEY = os.getenv("NEWS_API_KEY")
BASE_URL = "https://cryptonews-api.com/api/v1"


# # Search and Import News

def fetch_crypto_news(start_date, end_date):
    url = f"{BASE_URL}"
    params = {
        "token": API_KEY,
        "date": '-'.join([pd.Timestamp(dt).strftime('%d%m%Y') for dt in [start_date,end_date]]),
        "tickers": "BTC", 
        'items':3,
        "page":1,
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json().get("data", [])
    else:
        print(f"Error: {response.status_code}, {response.text}")
        return []


news = fetch_crypto_news(
    start_date = "2021-01-01",
    end_date = "2021-01-02",
)

news[1]

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
# -



# +
# Example usage
example_url = 'https://coingape.com/bitcoin-price-holds-in-consolidation-ahead-of-the-anticipated-liftoff-to-40000/'
article_details = extract_article_details(example_url)

# Display results
if "error" in article_details:
    print(f"Failed to extract article: {article_details['error']}")
else:
    print(f"Title: {article_details['title']}")
    print(f"Authors: {article_details['authors']}")
    print(f"Publish Date: {article_details['publish_date']}")
    print(f"Summary: {article_details.get('description', 'No summary available')}")
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



pip install transformers

