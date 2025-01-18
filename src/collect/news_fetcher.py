from typing import Optional
from datetime import timedelta
import pandas as pd
from pathlib import Path

from src.core.storage.delta_lake import DeltaLakeManager, TableNames
from src.collect.utils.news_api_caller import CryptoNewsFetcher
from src.core.logging.logger import setup_logger

logger = setup_logger("ImportNewsEndpoint", Path("crypto_news.log"))


class NewsImportEndpoint:
    """Endpoint for importing crypto news data."""
    
    def __init__(self):
        self.fetcher = CryptoNewsFetcher()
        self.deltalake = DeltaLakeManager()
        
    def _get_last_fetch_date(self) -> Optional[pd.Timestamp]:
        """Get the most recent date from existing data."""
        try:
            base_table = self.deltalake.read_table(
                table_name=TableNames.METADATA_ARTICLES.value,
                columns=['date_utc']
            )
            if not base_table.empty:
                date = pd.Timestamp(base_table['date_utc'].max()).tz_localize('UTC').tz_convert('US/Eastern')
                return date
            return None
        except Exception as e:
            logger.error(f"Error reading last fetch date: {e}")
            return None

    def _update_status_table(self, news_metadata: pd.DataFrame) -> None:
        """Update the status tracking table with new articles."""
        try:
            status_table = self.deltalake.read_table(
                table_name=TableNames.STATUS_ARTICLES.value, 
                columns=['news_id']
            )
            
            # Create status entries for new articles
            new_status = news_metadata.loc[
                ~news_metadata['news_id'].isin(status_table['news_id']),
                ['news_id']
            ].assign(**{
                TableNames.METADATA_ARTICLES.value: True,
                TableNames.SCRAPED_ARTICLES.value: False,
                TableNames.CLEANED_ARTICLES.value: False,
                TableNames.LLM_ARTICLES.value: False,
            }).reset_index(drop=True)
            
            if not new_status.empty:
                self.deltalake.write_table(
                    table_name=TableNames.STATUS_ARTICLES.value,
                    df=new_status
                )
                logger.info(f"Added {len(new_status)} new entries to status table")
        except Exception as e:
            logger.error(f"Error updating status table: {e}")
            raise

    def execute(self) -> dict:
        """Execute the news import process."""
        try:
            last_fetch_date = self._get_last_fetch_date()
            start_date = (last_fetch_date - timedelta(days=1))
            end_date = pd.Timestamp.now(tz = 'US/Eastern')
            
            logger.info(f"Fetching News...")
            
            # Fetch new data
            news_metadata = self.fetcher.fetch_news(start_date, end_date)
            
            # Filter out existing articles
            existing_articles = self.deltalake.read_table(
                table_name=TableNames.METADATA_ARTICLES.value,
                columns=['news_id']
            )
            news_metadata = news_metadata[
                ~news_metadata['news_id'].isin(existing_articles['news_id'])
            ]
            
            if news_metadata.empty:
                logger.info("No new unique articles found.")
                return {
                    "status": "success", 
                    "new_articles": 0,
                    "date_range": f"{start_date.date()} to {end_date.date()}"
                }
            
            # Persist metadata
            self.deltalake.write_table(
                table_name=TableNames.METADATA_ARTICLES.value,
                df=news_metadata
            )
            
            # Update status table
            self._update_status_table(news_metadata)
            
            return {
                "status": "success",
                "new_articles": len(news_metadata),
                "date_range": f"{start_date.date()} to {end_date.date()}"
            }
            
        except Exception as e:
            logger.error(f"Error in news import process: {e}")
            raise


def run_news_import() -> dict:
    """Entry point for the news import endpoint."""
    return NewsImportEndpoint().execute()


if __name__ == "__main__":
    run_news_import()
