from typing import Tuple, Set
import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from pathlib import Path
import pyprojroot
from logger_config import setup_logger

log_file = pyprojroot.here() / Path("logs/crypto_news.log")
logger = setup_logger("StoreCryptoNews", log_file)


def persist_news(news_df: pd.DataFrame, path:str) -> None:
    """
    Persist news data to Delta Lake format with upsert functionality.
    """
    logger.info(f"Persisting {news_df.shape[0]} news records")
    base_path = pyprojroot.here() / Path(path)
    str_path = str(base_path)
    
    if not (base_path / '_delta_log').exists():
        results = write_deltalake(
            table_or_uri=str_path,
            data=news_df,
            partition_by=['year_utc', 'month_utc', 'day_utc']
        )
        logger.info(f"New records inserted: {news_df.shape[0]}")
    else:
        dt = DeltaTable(str_path)
        merger = dt.merge(
            source=news_df,
            predicate="s.news_id = t.news_id",
            source_alias="s",
            target_alias="t"
        )
        merger.when_matched_update_all()
        merger.when_not_matched_insert_all()
        results = merger.execute()
        dt.vacuum(retention_hours=168)

        logger.info(f"New records inserted: {results['num_target_rows_inserted']}")
        logger.info(f"Existing records updated: {results['num_target_rows_updated']}")
