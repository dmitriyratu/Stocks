from typing import List, Dict, Tuple
from pathlib import Path
import numpy as np
import pandas as pd

from src.core.storage.delta_lake import DeltaLakeManager, TableNames
from src.clean.news.utils.text_summarizer import TextSummarizer
from src.clean.news.utils.text_processor import TextProcessor
from src.core.config import constants
from src.core.logging.logger import setup_logger

logger = setup_logger("ArticleCleanEndpoint", Path("crypto_news.log"))


class ArticleCleanEndpoint:
    """Endpoint for cleaning and processing article text."""
    
    def __init__(self):
        self.deltalake = DeltaLakeManager()
        self.text_processor = TextProcessor()
        self.text_summarizer = TextSummarizer()
        
    def _get_pending_articles(self) -> Tuple[pd.DataFrame, List[str]]:
        """Get articles pending cleaning from status table."""
        try:
            status_table = self.deltalake.read_table(
                table_name=TableNames.STATUS_ARTICLES.value,
                filters=[
                    (TableNames.METADATA_ARTICLES.value, "=", True),
                    (TableNames.SCRAPED_ARTICLES.value, "=", True),
                    (TableNames.CLEANED_ARTICLES.value, "=", False),
                ]
            )
            
            if status_table.empty:
                logger.info("No pending articles to clean")
                return pd.DataFrame(), []
                
            news_id_list = status_table['news_id'].tolist()
            return status_table, news_id_list
            
        except Exception as e:
            logger.error(f"Error fetching pending articles: {e}")
            raise
            
    def _fetch_article_data(self, news_id_list: List[str]) -> pd.DataFrame:
        """Fetch and combine metadata and scraped content."""
        try:
            # Fetch metadata
            news_metadata = self.deltalake.read_table(
                table_name=TableNames.METADATA_ARTICLES.value,
                filters=[("news_id", "in", news_id_list)]
            )
            
            # Fetch scraped articles
            news_articles = self.deltalake.read_table(
                table_name=TableNames.SCRAPED_ARTICLES.value,
                filters=[("news_id", "in", news_id_list)]
            )
            
            # Combine data
            return pd.merge(news_metadata, news_articles, how='left')
            
        except Exception as e:
            logger.error(f"Error fetching article data: {e}")
            raise
            
    def _clean_text(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and preprocess article text."""
        try:
            # Clean full text
            df['full_cleaned_text'] = df['full_text'].map(self.text_processor.clean_text)
            
            # Generate curated text
            df[['full_cleaned_text', 'error', 'spam_score']] = df.apply(
                lambda row: self.text_processor.generate_curated_text(
                    row['full_cleaned_text'], 
                    row['error']
                ), 
                axis=1
            ).apply(pd.Series)
            
            # Calculate word and token counts
            for text_type in ['preview_text', 'full_cleaned_text']:
                df[
                    [text_type + '_word_count', text_type + '_token_count']
                ] = df[text_type].map(self.text_processor.measure_text).apply(pd.Series)
                
            return df
            
        except Exception as e:
            logger.error(f"Error cleaning text: {e}")
            raise
            
    def _engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Engineer features from cleaned text."""
        try:
            # Select best text version based on word count
            mask = (
                df['preview_text_word_count'].fillna(0).gt(
                    df['full_cleaned_text_word_count'].fillna(0)
                ) & 
                df['preview_text_word_count'].ge(constants.MINIMUM_ARTICLE_WORDS)
            )
            df['selected_text'] = df['preview_text'].where(mask, df['full_cleaned_text'])
            
            # Calculate metrics for selected text
            df[['selected_text_word_count', 'selected_text_token_count']] = (
                df['selected_text'].map(self.text_processor.measure_text).apply(pd.Series)
            )
            
            # Generate LLM-ready text
            df['llm_ready_text'] = df['selected_text'].map(self.text_summarizer.text_summarize)
            df[['llm_ready_text_word_count', 'llm_ready_text_token_count']] = (
                df['llm_ready_text'].map(self.text_processor.measure_text).apply(pd.Series)
            )
            
            # Select final columns
            return df[
                ['news_id', 'date', 'date_utc', 'year_utc', 'month_utc', 'day_utc'] +
                np.concatenate([
                    [i, i + '_word_count', i + '_token_count'] for i in
                    ['selected_text', 'llm_ready_text']
                ]).tolist()
            ]
            
        except Exception as e:
            logger.error(f"Error engineering features: {e}")
            raise
            
    def _persist_results(self, cleaned_data: pd.DataFrame, status_table: pd.DataFrame) -> None:
        """Persist cleaned data and update status."""
        try:
            # Write cleaned data
            self.deltalake.write_table(
                table_name=TableNames.CLEANED_ARTICLES.value,
                df=cleaned_data
            )
            
            # Update status table
            status_table.loc[
                status_table['news_id'].isin(cleaned_data['news_id']),
                TableNames.CLEANED_ARTICLES.value
            ] = True
            
            self.deltalake.write_table(
                table_name=TableNames.STATUS_ARTICLES.value,
                df=status_table
            )
            
            logger.info(f"Successfully persisted {len(cleaned_data)} cleaned articles")
            
        except Exception as e:
            logger.error(f"Error persisting results: {e}")
            raise
            
    def execute(self) -> Dict:
        """Execute the article cleaning process."""
        try:
            # Get pending articles
            status_table, news_id_list = self._get_pending_articles()
            
            if not news_id_list:
                return {
                    "status": "success",
                    "articles_cleaned": 0,
                    "message": "No pending articles to clean"
                }
            
            # Fetch and combine article data
            cleaning_data = self._fetch_article_data(news_id_list)
            
            # Clean text
            logger.info("Cleaning Data...")
            cleaning_data = self._clean_text(cleaning_data)
            
            # Engineer features
            logger.info("Feature Engineering Data...")
            cleaned_data = self._engineer_features(cleaning_data)
            
            # Persist results
            self._persist_results(cleaned_data, status_table)
            
            return {
                "status": "success",
                "articles_cleaned": len(cleaned_data),
            }
            
        except Exception as e:
            logger.error(f"Error in article cleaning process: {e}")
            raise


def run_article_cleaning() -> Dict:
    """Entry point for the article cleaning endpoint."""
    return ArticleCleanEndpoint().execute()


if __name__ == "__main__":
    run_article_cleaning()
