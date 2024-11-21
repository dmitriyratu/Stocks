import requests
import os
from dotenv import load_dotenv
import datetime

# # Setup

load_dotenv()
API_KEY = os.getenv("NEWS_API_KEY")

# +
today = datetime.datetime.now()
yesterday = today - datetime.timedelta(days=1)

query = "bitcoin"

url = f"https://newsapi.org/v2/everything?q={query}&from={from_date}&to={to_date}&sortBy=publishedAt&apiKey={api_key}"

print(url)
# -

# # Import Data

response = requests.get(url)
news_data = response.json()


news_data

# +

# Extract headlines
headlines = [article['title'] for article in news_data['articles']]


# +
BASE_URL = "https://api.goperigon.com/v1/all"

from_date = (today - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
to_date = datetime.datetime.now().strftime('%Y-%m-%d')

# Parameters for the request
params = {
    "apiKey": API_KEY,
    "q": "bitcoin", 
    "from": from_date,
    "to": to_date,
    "sortBy": "date",
    # "source": "cnn.com",  # (Optional) Filter by specific source
    "language": 'en',
    "fullText": "true",
}

resp = requests.get(BASE_URL, params=params)
resp_json = resp.json()

print(f"Result count: {resp_json['numResults']}")
# -



[i['source']['domain'] for i in resp_json['articles']]

[i['title'] for i in resp.json()['articles']]

{i:resp_json['articles'][0][i] for i in ['medium','title','summary','sentiment']}

resp_json['articles'][0]
