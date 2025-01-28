from prefect import flow
from typing import Dict
from prefect.task_runners import ConcurrentTaskRunner
from prefect.logging import get_run_logger

from src.flows.news_processing.tasks import news_tasks


@flow(
    name="crypto_news_processing",
    task_runner=ConcurrentTaskRunner(),
    description="Process crypto news from import to LLM analysis",
)
def process_news(environment: str) -> Dict:
    """Main flow for complete news processing pipeline"""

    logger = get_run_logger()

    logger.info(f"Processing news in the {environment.upper()} environment.")

    try:
        import_result = news_tasks.import_news()
        scrape_result = news_tasks.scrape_articles(wait_for=[import_result])
        clean_result = news_tasks.clean_articles(wait_for=[scrape_result])
        
        return {
            "import": import_result,
            "scrape": scrape_result,
            "clean": clean_result,
        }
    except:
        logger.error(f"Error in news processing: {str(e)}")
        return {
            "status": "error",
            "error": str(e)
        }


if __name__ == "__main__":
    process_news.serve(name="dev-deployment", tags=["dev"])
