from prefect import task
from prefect.logging import get_run_logger
from typing import Dict

from src.collect.news_fetcher import NewsImportEndpoint
from src.collect.article_scraper import ArticleScrapeEndpoint
from src.clean.article_cleaner import ArticleCleanEndpoint


# +
@task(name="import_news")
def import_news() -> Dict:
    """Task to import news from API"""
    logger = get_run_logger()
    logger.info("Calling NewsImportEndpoint...")
    result = NewsImportEndpoint().execute()
    logger.info(f"Import completed: {result}")
    return result

@task(name="scrape_articles")
def scrape_articles() -> Dict:
    """Task to scrape article content"""
    logger = get_run_logger()
    logger.info("Calling ArticleScrapeEndpoint...")
    result = ArticleScrapeEndpoint().execute()
    logger.info(f"Scraping completed: {result}")
    return result

@task(name="clean_articles")
def clean_articles() -> Dict:
    """Task to clean article text"""
    logger = get_run_logger()
    logger.info("Calling ArticleCleanEndpoint...")
    result = ArticleCleanEndpoint().execute()
    logger.info(f"Cleaning completed: {result}")
    return result
