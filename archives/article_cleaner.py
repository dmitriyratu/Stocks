from pathlib import Path
from deltalake import DeltaTable
import pyprojroot
import pandas as pd
import numpy as np

from src.core.storage.delta_lake import DeltaLakeManager, TableNames
from src.clean.utils.text_summarizer import TextSummarizer
from src.clean.utils.text_processor import TextProcessor
from src.clean.utils.spam_detector import SpamDetector
from src.core.config import constants

deltalake = DeltaLakeManager()

# # Import Data

# ## Import Status Data

status_table = deltalake.read_table(
    table_name = TableNames.STATUS_ARTICLES, 
    filters = [
        (TableNames.METADATA_ARTICLES.value, "=", True),
        (TableNames.SCRAPED_ARTICLES.value, "=", True),
        (TableNames.CLEANED_ARTICLES.value, "=", False),
    ]
)

news_id_list = status_table['news_id'].tolist()

# ## Import News Metadata

news_metadata = deltalake.read_table(table_name = TableNames.METADATA_ARTICLES, filters = [("news_id", "in", news_id_list)])

# ## Import Scraped Articles

news_articles = deltalake.read_table(table_name = TableNames.SCRAPED_ARTICLES, filters = [("news_id", "in", news_id_list)])

# ## Combine Data

cleaning_data = pd.merge(
    news_metadata,
    news_articles,
    how = 'left',
)

# # Preprocess Text

tprocessor = TextProcessor()

# +
cleaning_data['full_cleaned_text'] = cleaning_data['full_text'].map(tprocessor.clean_text)

cleaning_data[['full_cleaned_text','error','spam_score']] = cleaning_data.apply(lambda row: tprocessor.generate_curated_text(row['full_cleaned_text'], row['error']), axis=1).apply(pd.Series)
# -

for text_type in ['preview_text','full_cleaned_text']:
    cleaning_data[
        [text_type + '_word_count',text_type + '_token_count']
    ] = cleaning_data[text_type].map(tprocessor.measure_text).apply(pd.Series)

# # Feature Engineer

mask = cleaning_data['preview_text_word_count'].fillna(0).gt(cleaning_data['full_cleaned_text_word_count'].fillna(0)) & cleaning_data['preview_text_word_count'].ge(constants.MINIMUM_ARTICLE_WORDS)
cleaning_data['selected_text'] = cleaning_data['preview_text'].where(mask, cleaning_data['full_cleaned_text'])

cleaning_data[['selected_text_word_count','selected_text_token_count']] = cleaning_data['selected_text'].map(tprocessor.measure_text).apply(pd.Series)

summarizer = TextSummarizer()
cleaning_data['llm_ready_text'] = cleaning_data['selected_text'].map(summarizer.text_summarize)

cleaning_data[['llm_ready_text_word_count','llm_ready_text_token_count']] = cleaning_data['llm_ready_text'].map(tprocessor.measure_text).apply(pd.Series)

cleaned_data = cleaning_data[
    ['news_id','date','date_utc','year_utc','month_utc','day_utc'] + 
    np.concatenate([
        [i, i + '_word_count', i + '_token_count'] for i in
        ['selected_text','llm_ready_text']
    ]).tolist()
]

# # Persist Data

# ## Cleaned Data

deltalake.write_table(
    table_name = TableNames.CLEANED_ARTICLES,
    df = cleaned_data
)

# ## Status Data

status_table.loc[status_table['news_id'].isin(news_id_list), TableNames.CLEANED_ARTICLES.value] = True
deltalake.write_table(
    table_name = TableNames.STATUS_ARTICLES,
    df = status_table
)
