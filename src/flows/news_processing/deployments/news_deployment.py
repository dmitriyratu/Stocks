from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

from src.flows.news_processing.pipelines.news_pipeline import process_news

if __name__ == "__main__":
    deployment = Deployment.build_from_flow(
        flow=process_news,
        name="crypto_news_processing",
        version="1",
        work_queue_name="default",
        schedule=None  # We'll add scheduling later
    )
    
    deployment.apply()
