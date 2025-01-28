from prefect import task
from prefect.logging import get_run_logger
from typing import Dict

from src.collect.news.news_fetcher import NewsImportEndpoint
from src.collect.news.article_scraper import ArticleScrapeEndpoint
from src.clean.news.article_cleaner import ArticleCleanEndpoint


# +
@task(name="import_news", retries=2, retry_delay_seconds=30)
def import_news() -> Dict:
    """Task to import news from API"""
    logger = get_run_logger()
    try:
        logger.info("Calling NewsImportEndpoint...")
        result = NewsImportEndpoint().execute()
        logger.info(f"Import completed: {result}")
        return result
    except Exception as e:
        logger.error(f"Error importing news: {str(e)}")
        raise

@task(name="scrape_articles", retries=2, retry_delay_seconds=30)
def scrape_articles() -> Dict:
    """Task to scrape article content"""
    logger = get_run_logger()
    try:
        logger.info("Calling ArticleScrapeEndpoint...")
        result = ArticleScrapeEndpoint().execute()
        logger.info(f"Scraping completed: {result}")
        return result
    except Exception as e:
        logger.error(f"Error scraping articles: {str(e)}")
        raise

@task(name="clean_articles", retries=2, retry_delay_seconds=30)
def clean_articles() -> Dict:
    """Task to clean article text"""
    logger = get_run_logger()
    try:
        logger.info("Calling ArticleCleanEndpoint...")
        result = ArticleCleanEndpoint().execute()
        logger.info(f"Cleaning completed: {result}")
        return result
    except Exception as e:
        logger.error(f"Error cleaning articles: {str(e)}")
        raise
