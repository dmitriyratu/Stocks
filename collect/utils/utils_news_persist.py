from typing import Tuple, Set, Optional, List
from dataclasses import dataclass, field
from enum import Enum
import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from pathlib import Path
import pyprojroot
from logger_config import setup_logger

log_file = pyprojroot.here() / Path("logs/delta_lake.log")
logger = setup_logger("DeltaLakeManager", log_file)


# +
class TableNames(Enum):
    METADATA_ARTICLES = "raw_data"
    SCRAPED_ARTICLES = "scraped_data"
    CLEANED_ARTICLES = "cleaned_data"
    LLM_ARTICLES = "llm_data"

@dataclass
class TableSchema:
    name: str
    predicate: str 
    base_path: Path
    universal_path: Optional[Path] = None
    partition_columns: List[str] = field(default_factory=list)   


# +
class DeltaLakeManager:
    """Manages Delta Lake tables for the news processing pipeline."""
    
    def __init__(self, base_path: str):

        self.root = pyprojroot.here()
        self.table_schemas = self.init_tables()

    def init_tables(self):

        return {
            TableNames.METADATA_ARTICLES: TableSchema(
                name=TableNames.METADATA_ARTICLES.value,
                predicate = "news_id",
                base_path = self.root / Path('data/news/BTC/raw_data/'),
                universal_path = self.root / Path('data/news/BTC'),
                partition_columns=['year_utc', 'month_utc', 'day_utc'],
            ),
            TableNames.SCRAPED_ARTICLES: TableSchema(
                name=TableNames.SCRAPED_ARTICLES.value,
                predicate = "news_id",
                base_path = self.root / Path('data/news/BTC/scraped_data'),
                universal_path = self.root / Path('data/news/BTC'),
                partition_columns=['year_utc', 'month_utc', 'day_utc'],
            ),
            TableNames.CLEANED_ARTICLES: TableSchema(
                name=TableNames.CLEANED_ARTICLES.value,
                predicate = "news_id",
                base_path = self.root / Path('data/news/BTC/cleaned_data'),
                universal_path = self.root / Path('data/news/BTC'),
                partition_columns=['year_utc', 'month_utc', 'day_utc'],
            ),
            TableNames.LLM_ARTICLES: TableSchema(
                name=TableNames.LLM_ARTICLES.value,
                predicate = "news_id",
                base_path = self.root / Path('data/news/BTC/llm_data'),
                universal_path = self.root / Path('data/news/BTC'),
                partition_columns=['year_utc', 'month_utc', 'day_utc'],
            )
        }
        
    def persist_news(self, table_name: TableNames, df: pd.DataFrame) -> None:
        """
        Persist news data to Delta Lake format with upsert functionality.
        """
        
        logger.info(f"Identified {table_name.name}: {len(df)} rows")

        table_config = self.init_tables[table_name]
        
        if not (table_config.base_path / '_delta_log').exists():
            results = write_deltalake(
                table_or_uri=str(table_config.base_path),
                data=df,
                partition_by=table_config.partition
            )
            logger.info(f"Inserted: {len(df)} rows")
        else:
            dt = DeltaTable(str(table_config.base_path))
            merger = dt.merge(
                source=df,
                predicate=f"s.{table_config.predicate} = t.{table_config.predicate}",
                source_alias="s",
                target_alias="t"
            )
            merger.when_matched_update_all()
            merger.when_not_matched_insert_all()
            results = merger.execute()
            dt.vacuum(retention_hours=168)
    
            logger.info(f"Inserted: {results['num_target_rows_inserted']} rows")
            logger.info(f"Updated: {results['num_target_rows_updated']} rows")

        # if table_config.universal_path:

        #     universal_table = pd.DataFrame()
            
        #     if not (table_config.universal_path / '_delta_log').exists():
        #         results = write_deltalake(
        #             table_or_uri=str(table_config.base_path),
        #             data=universal_table,
        #         )
            
            
# -


