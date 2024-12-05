import datetime
import os

import pandas as pd
import requests
from dotenv import load_dotenv

pd.set_option('display.max_columns', None)

# # Setup

# +
load_dotenv()
API_KEY = os.getenv("NEWS_API_KEY")

BASE_URL = "https://api.goperigon.com/v1/all"
# -

# # Search and Import News

# +
today = datetime.datetime.now()

to_date = today.strftime('%Y-%m-%d')
from_date = (today - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

params = {
    "apiKey": API_KEY,
    "q": "bitcoin", 
    "from": from_date,
    "to": to_date,
    "sortBy": "date",
    # "source": "cnn.com",
    "language": 'en',
    "fullText": "true",
}

resp = requests.get(BASE_URL, params=params)
resp_json = resp.json()

print(f"Result count: {resp_json['numResults']}")
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




