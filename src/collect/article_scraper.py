from typing import List, Dict, Optional
from pathlib import Path
from more_itertools import chunked
from tqdm.notebook import tqdm
import pandas as pd

from src.core.storage.delta_lake import DeltaLakeManager, TableNames
from src.collect.utils.article_url_scraper import PowerScraper, ScrapingResult
from src.core.logging.logger import setup_logger

logger = setup_logger("ArticleScrapeEndpoint", Path("crypto_news.log"))


class ArticleScrapeEndpoint:
    """Endpoint for scraping article content from news URLs."""
    
    CHUNK_SIZE = 100
    
    def __init__(self):
        self.deltalake = DeltaLakeManager()
    
    def _get_pending_articles(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Get articles pending scraping from status table."""
        try:
            # Get articles marked for scraping
            status_table = self.deltalake.read_table(
                table_name=TableNames.STATUS_ARTICLES.value,
                filters=[
                    (TableNames.METADATA_ARTICLES.value, "=", True),
                    (TableNames.SCRAPED_ARTICLES.value, "=", False),
                ]
            )
            
            if status_table.empty:
                logger.info("No pending articles to scrape")
                return pd.DataFrame(), pd.DataFrame()
            
            news_id_list = status_table['news_id'].tolist()
            
            # Get corresponding metadata
            news_metadata = self.deltalake.read_table(
                table_name=TableNames.METADATA_ARTICLES.value,
                filters=[("news_id", "in", news_id_list)]
            )
            
            return status_table, news_metadata
            
        except Exception as e:
            logger.error(f"Error fetching pending articles: {e}")
            raise
    
    def _scrape_urls(self, urls: List[str]) -> List[ScrapingResult]:
        """Scrape content from URLs in chunks with progress tracking."""
        try:
            url_chunks = list(chunked(urls, self.CHUNK_SIZE))
            all_results = []
            total_success = 0
            
            with tqdm(total=len(urls)) as pbar:
                for chunk in url_chunks:
                    with PowerScraper() as scraper:
                        results = scraper.scrape_urls(chunk)
                        all_results.extend(results)
                        
                        chunk_success = sum(1 for r in results if r.success)
                        total_success += chunk_success
                        
                        pbar.update(len(chunk))
                        pbar.set_postfix(
                            chunk=f"{chunk_success}/{len(chunk)}",
                            total=f"{total_success}/{len(all_results)} ({total_success/len(all_results)*100:.1f}%)"
                        )
            
            return all_results
            
        except Exception as e:
            logger.error(f"Error during URL scraping: {e}")
            raise
    
    def _persist_results(
        self, 
        news_metadata: pd.DataFrame, 
        scraping_results: List[ScrapingResult],
        status_table: pd.DataFrame
    ) -> None:
        """Persist scraped content and update status."""
        try:
            # Merge metadata with scraping results
            news_articles = pd.merge(
                news_metadata[['news_id', 'news_url', 'date_utc', 'year_utc', 'month_utc', 'day_utc']],
                pd.DataFrame(scraping_results),
                how='left'
            )
            
            # Write scraped content
            self.deltalake.write_table(
                table_name=TableNames.SCRAPED_ARTICLES.value,
                df=news_articles
            )
            
            # Update status table
            status_table.loc[
                status_table['news_id'].isin(news_metadata['news_id']), 
                TableNames.SCRAPED_ARTICLES.value
            ] = True
            
            self.deltalake.write_table(
                table_name=TableNames.STATUS_ARTICLES.value,
                df=status_table
            )
            
            logger.info(f"Successfully persisted {len(news_articles)} articles")
            
        except Exception as e:
            logger.error(f"Error persisting results: {e}")
            raise
    
    def execute(self) -> Dict:
        """Execute the article scraping process."""
        
        try:
            # Get pending articles
            status_table, news_metadata = self._get_pending_articles()
            
            if news_metadata.empty:
                return {
                    "status": "success",
                    "articles_scraped": 0,
                    "message": "No pending articles to scrape"
                }
            
            # Scrape URLs
            urls = pd.unique(news_metadata['news_url']).tolist()
            scraping_results = self._scrape_urls(urls)
            
            # Persist results
            self._persist_results(news_metadata, scraping_results, status_table)
            
            successful_scrapes = sum(1 for r in scraping_results if r.success)
            return {
                "status": "success",
                "articles_scraped": len(scraping_results),
                "successful_scrapes": successful_scrapes,
                "success_rate": f"{(successful_scrapes/len(scraping_results))*100:.1f}%"
            }
            
        except Exception as e:
            logger.error(f"Error in article scraping process: {e}")
            raise


def run_article_scraping() -> Dict:
    """Entry point for the article scraping endpoint."""
    return ArticleScrapeEndpoint().execute()


if __name__ == "__main__":
    run_article_scraping()
