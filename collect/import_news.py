from pathlib import Path
from transformers import pipeline
import torch_directml
from dotenv import load_dotenv
from logger_config import setup_logger
from pandarallel import pandarallel
import pandas as pd
import os
from webdriver_manager.chrome import ChromeDriverManager


from collect.utils.utils_news_fetcher import CryptoNewsFetcher

# # Configuration

load_dotenv()
pandarallel.initialize(nb_workers= os.cpu_count() - 1, verbose = 0)


log_file = Path("../logs/crypto_news.log")
logger = setup_logger("GetCryptoNews", log_file)

# # Import News URL's


fetcher = CryptoNewsFetcher(logger)

news_df = fetcher.fetch_news("2021-01-01", "2021-01-02")



# +
# %%time

scraper = ContentScraper(logger = logger)

news_df[['scraper', 'text']] = pd.DataFrame(
    news_df['news_url']
    .parallel_apply(scraper.get_article_content)
    .tolist()
)
# +
mask = news_df['text'].isna()
news_df.loc[mask, ['scraper', 'text']] = pd.DataFrame(
    news_df['news_url'][mask].apply(scraper.get_article_content).tolist()
)

scraper.close_driver()
# -




from collect.utils.test import PowerScraper

scraper = PowerScraper()

# %%time
output = news_df['news_url'].apply(scraper.scrape)

pd.DataFrame([i for i in output])











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
