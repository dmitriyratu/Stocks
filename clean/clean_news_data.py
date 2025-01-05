from pathlib import Path
from deltalake import DeltaTable
import pyprojroot
import pandas as pd

from clean.utils.utils_text_summarizer import TextSummarizer
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

# +
news_data['full_cleaned_text'] = news_data['full_text'].map(tprocessor.clean_text)

news_data[['full_cleaned_text','error','spam_score']] = news_data.apply(lambda row: tprocessor.generate_curated_text(row['full_cleaned_text'], row['error']), axis=1).apply(pd.Series)
# -

for text_type in ['preview_text','full_cleaned_text']:
    news_data[
        [text_type + '_word_count',text_type + '_token_count']
    ] = news_data[text_type].map(tprocessor.measure_text).apply(pd.Series)

# # Feature Engineer

mask = news_data['preview_text_word_count'].fillna(0).gt(news_data['full_cleaned_text_word_count'].fillna(0)) & news_data['preview_text_word_count'].ge(constants.MINIMUM_ARTICLE_WORDS)
news_data['selected_text'] = news_data['preview_text'].where(mask, news_data['full_cleaned_text'])

news_data[['selected_text_word_count','selected_text_token_count']] = news_data['selected_text'].map(tprocessor.measure_text).apply(pd.Series)

# +
summarizer = TextSummarizer()

news_data['llm_ready_text'] = news_data['selected_text'].map(summarizer.text_summarize)

news_data[['llm_ready_text_word_count','llm_ready_text_token_count']] = news_data['llm_ready_text'].map(tprocessor.measure_text).apply(pd.Series)
# -









token_count

len(summarizer.text_summarize(text, token_count))/4
