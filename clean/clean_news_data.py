from pathlib import Path
from deltalake import DeltaTable
import pyprojroot
import pandas as pd

from clean.utils.utils_text_summarizer import text_summarize
from clean.utils.utils_text_post_processor import TextProcessor
from clean.utils.utils_spam_detector import SpamDetector
from config import constants

# # Import News Data

base_path = pyprojroot.here() / Path('data/news/BTC/raw_data/')
dt = DeltaTable(str(base_path))
filters = [
    ('year_utc', '=', 2024),
    ('month_utc', '=', 12),
]
news_metadata = dt.to_pyarrow_table(filters=filters).to_pandas()

base_path = pyprojroot.here() / Path('data/news/BTC/scraped_data/')
dt = DeltaTable(str(base_path))
filters = [
    ('year_utc', '=', 2024),
    ('month_utc', '=', 12),
]
news_articles = dt.to_pyarrow_table(filters=filters).to_pandas()

# ## Combine Data

news_data = pd.merge(
    news_metadata,
    news_articles,
    how = 'left',
)

# # Preprocess Text

tprocessor = TextProcessor()

news_data['full_cleaned_text'] = news_data['full_text'].map(tprocessor.clean_text)
news_data[['full_cleaned_text','error','spam_score']] = news_data.apply(lambda row: tprocessor.generate_curated_text(row['full_cleaned_text'], row['error']), axis=1).apply(pd.Series)
news_data[['full_cleaned_word_count','full_cleaned_token_count']] = news_data['full_cleaned_text'].map(tprocessor.measure_text).apply(pd.Series)

# # Feature Engineer

mask = news_data['preview_word_count'].fillna(0).gt(news_data['full_cleaned_word_count'].fillna(0)) & news_data['preview_word_count'].ge(constants.MINIMUM_ARTICLE_WORDS)
news_data['selected_text'] = news_data['preview_text'].where(mask, news_data['full_cleaned_text'])

news_data['selected_text']

news_data[['selected_word_count','selected_text_token_count']] = news_data['selected_text'].map(tprocessor.measure_text).apply(pd.Series)




word_count
word_count
word_count
# +word_count
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
