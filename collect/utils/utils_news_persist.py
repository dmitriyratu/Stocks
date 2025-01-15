from typing import Tuple, Set, Optional, List, Dict
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
    STATUS_ARTICLES = 'article_status_data'

@dataclass
class TableSchema:
    name: str
    predicate: str
    base_path: Path
    universal_path: Optional[Path] = None
    partition_columns: List[str] = field(default_factory=list)


# -

class DeltaLakeManager:
    """Manages Delta Lake tables for the news processing pipeline."""
    
    def __init__(self):

        self.root = pyprojroot.here()
        self.table_schemas = self._init_tables()

    def _init_tables(self) -> Dict[TableNames, TableSchema]:

        return {
            TableNames.STATUS_ARTICLES: TableSchema(
                name=TableNames.STATUS_ARTICLES.value,
                predicate = "news_id",
                base_path = self.root / Path('data/news/BTC/process_status'),
                partition_columns=[],
            ),
            TableNames.METADATA_ARTICLES: TableSchema(
                name=TableNames.METADATA_ARTICLES.value,
                predicate = "news_id",
                base_path = self.root / Path('data/news/BTC/raw_data/'),
                partition_columns=['year_utc', 'month_utc', 'day_utc'],
            ),
            TableNames.SCRAPED_ARTICLES: TableSchema(
                name=TableNames.SCRAPED_ARTICLES.value,
                predicate = "news_id",
                base_path = self.root / Path('data/news/BTC/scraped_data'),
                partition_columns=['year_utc', 'month_utc', 'day_utc'],
            ),
            TableNames.CLEANED_ARTICLES: TableSchema(
                name=TableNames.CLEANED_ARTICLES.value,
                predicate = "news_id",
                base_path = self.root / Path('data/news/BTC/cleaned_data'),
                partition_columns=['year_utc', 'month_utc', 'day_utc'],
            ),
            TableNames.LLM_ARTICLES: TableSchema(
                name=TableNames.LLM_ARTICLES.value,
                predicate = "news_id",
                base_path = self.root / Path('data/news/BTC/llm_data'),
                partition_columns=['year_utc', 'month_utc', 'day_utc'],
            ),   
        }

    def _create_table(self, path:Path, data: pd.DataFrame, partition_columns: Optional[List[str]] = None) -> None:
        """Create a new Delta table"""
        write_args = {"table_or_uri": str(path), "data": data}
        if partition_columns:
            write_args["partition_by"] = partition_columns
        write_deltalake(**write_args)

    def _merge_table(self, path:Path, data: pd.DataFrame, predicate: str) -> dict:
        """Merge data into existing Delta table"""
        
        table = DeltaTable(str(path))
        
        merger = table.merge(
            source=data,
            predicate=f"s.{predicate} = t.{predicate}",
            source_alias="s",
            target_alias="t"
        )
        merger.when_matched_update_all()
        merger.when_not_matched_insert_all()
        results = merger.execute()

        table.vacuum(retention_hours=168)
        
        return results
        
    def write_table(self, table_name: TableNames, df: pd.DataFrame) -> None:
        """
        Persist data to Delta Lake format with upsert functionality.
        """

        if df.empty:
            logger.warning(f"Empty DataFrame provided for {table_name.value}, skipping persist")
            return

        table_config = self.table_schemas.get(table_name)

        logger.info(f"Persisting {len(df)} rows to {table_name.value}")
        
        if not (table_config.base_path / '_delta_log').exists():
            self._create_table(table_config.base_path, df, table_config.partition_columns)
            logger.info(f"Created new table with {len(df)} rows")
        else:
            results = self._merge_table(table_config.base_path, df, table_config.predicate)
            logger.info(f"Merged data: {results['num_target_rows_inserted']} rows inserted, "
                       f"{results['num_target_rows_updated']} rows updated")
            
    def read_table(
        self, table_name: TableNames, 
        filters: Optional[List[tuple]] = None, 
        columns: Optional[List[tuple]] = None
    ) -> pd.DataFrame:
        """
        Read data from a Delta table with optional filters
        """
        
        table_config = self.table_schemas.get(table_name)

        if not (table_config.base_path / '_delta_log').exists():
            logger.warning(f"Table {table_name.value} does not exist")
            return pd.DataFrame(columns = columns)

        dt = DeltaTable(str(table_config.base_path))
        return dt.to_pandas(filters=filters, columns = columns)
