from pathlib import Path
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from logger_config import setup_logger
import pandas as pd
import pyprojroot

log_file = pyprojroot.here() / Path("logs/crypto_news.log")
logger = setup_logger("StoreCryptoNews", log_file)


def persist_news(news_df: pd.DataFrame) -> None:
    """Persist news data to Delta Lake format with upsert functionality."""

    logger.info(f"Starting to persist {news_df.shape[0]} news records")

    base_path = pyprojroot.here() / Path('data/news/crypto_news')
    
    pa_table = pa.Table.from_pandas(news_df)
    schema_dict = {field.name: field.type for field in pa_table.schema}
    
    overrides = {
        'date_utc': pa.timestamp('us'),
    }
    
    schema = pa.schema([
        (name, overrides.get(name, dtype))
        for name, dtype in schema_dict.items()
    ])
    
    path_str = str(base_path)
    
    if not (base_path / '_delta_log').exists():
        logger.info(f"Creating new Delta Lake table at {base_path}")
        dt = DeltaTable.create(
            path_str,
            schema=schema,
            partition_by=['year_utc', 'month_utc']
        )
        existing_ids = set()
    else:
        logger.info(f"Using existing Delta Lake table at {base_path}")
        dt = DeltaTable(path_str)
        existing_ids = set(dt.to_pandas()['news_id'])

        
    new_ids = set(news_df['news_id'])
    to_insert = new_ids - existing_ids
    to_update = new_ids & existing_ids
    
    dt.merge(
        source=news_df,
        predicate="s.news_id = t.news_id",
        source_alias="s",
        target_alias="t"
    )
    
    logger.info(f"Merge complete. Merge operation summary:")
    logger.info(f"\t New records inserted: {len(to_insert)}")
    logger.info(f"\t Existing `news_id` records updated: {len(to_update)}")

    return to_insert, to_update
