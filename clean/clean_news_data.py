from pathlib import Path
from deltalake import DeltaTable
import pyprojroot
import pandas as pd
import cleantext
from config import parameters, constants
from transformers import GPT2TokenizerFast

from clean.utils import utils_text_summarizer

# # Init

tokenizer = GPT2TokenizerFast.from_pretrained("gpt2")

# # Import News Data

base_path = pyprojroot.here() / Path('data/news/BTC/raw_data/raw_data/year_utc=2024/month_utc=12')
news_metadata = DeltaTable(str(base_path)).to_pandas()

base_path = pyprojroot.here() / Path('data/news/BTC/scraped_url_data')
news_articles = DeltaTable(str(base_path)).to_pandas()

# ## Combine Data

news_data = pd.merge(
    news_metadata,
    news_articles,
    how = 'left'
)

# # Feature Engineer

# +
news_data[['selected_text', 'selected_text_size', 'imputed_text_flag']]  = news_data.apply(
    lambda x: (x['preview_text'], x['preview_text_size'], True) 
    if pd.isna(x['full_text']) and x['preview_text_size'] > constants.MINIMUM_ARTICLE_WORDS
    else (x['full_text'], x['full_text_size'], False), 
    axis=1,
    result_type='expand'
)

news_data = news_data.dropna(subset = ['selected_text'], ignore_index = True)
news_data.loc[:,'cleaned_text'] = news_data['selected_text'].apply(cleantext.clean, **parameters.TEXT_CLEANING_PARAMS)

news_data.loc[:,'cleaned_text_token_count'] = news_data['cleaned_text'].map(
    lambda text: len(tokenizer.encode(text, add_special_tokens=False, truncation = False, verbose=False))
)

news_data.loc[:, 'llm_ready_text'] = news_data.apply(
    lambda row: utils_text_summarizer.summarize(row['cleaned_text'], row['cleaned_text_token_count']), axis=1
)

news_data.loc[:, 'llm_ready_text_token_count'] = news_data['llm_ready_text'].map(
    lambda text: len(tokenizer.encode(text, add_special_tokens=False, truncation = False, verbose=False))
)
