from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional, List, Dict, Any
import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
import pyprojroot
from logger_config import setup_logger

log_file = pyprojroot.here() / Path("logs/delta_lake.log")
logger = setup_logger("DeltaLakeManager", log_file)


# +
class TableNames(Enum):
    RAW_METADATA = "raw_data"
    SCRAPED_ARTICLES = "scraped_data"
    CLEANED_ARTICLES = "cleaned_data"
    LLM_READY = "llm_data"

@dataclass
class TableSchema:
    name: str
    partition_columns: List[str]

class DeltaLakeManager:
    """Manages Delta Lake tables for the news processing pipeline."""
    
    def __init__(self, base_path: Path = None):
        self.base_path = base_path
        
        # Define schemas for each table
        self.table_schemas = {
            TableNames.RAW_METADATA: TableSchema(
                name="raw_data",
                partition_columns=['year_utc', 'month_utc', 'day_utc'],
                required_columns=[
                    'news_id', 'date', 'date_utc', 'year_utc', 'month_utc', 'day_utc',
                    'type', 'source_name', 'tickers', 'topics', 'news_url', 'rank_score',
                    'news_api_sentiment', 'title_text', 'preview_text'
                ]
            ),
            TableNames.SCRAPED_ARTICLES: TableSchema(
                name="scraped_data",
                partition_columns=['year_utc', 'month_utc', 'day_utc'],
                required_columns=[
                    'news_id', 'news_url', 'date_utc', 'year_utc', 'month_utc', 'day_utc',
                    'full_text', 'error', 'success'
                ]
            ),
            TableNames.CLEANED_ARTICLES: TableSchema(
                name="cleaned_data",
                partition_columns=['year_utc', 'month_utc', 'day_utc'],
                required_columns=[
                    'news_id', 'date_utc', 'year_utc', 'month_utc', 'day_utc',
                    'selected_text', 'selected_text_word_count', 'selected_text_token_count',
                    'llm_ready_text', 'llm_ready_text_word_count', 'llm_ready_text_token_count'
                ]
            ),
            TableNames.PROCESSING_STATE: TableSchema(
                name="processing_state",
                partition_columns=['year_utc', 'month_utc', 'day_utc'],
                required_columns=[
                    'news_id', 'status', 'last_updated', 'error',
                    'year_utc', 'month_utc', 'day_utc'
                ]
            )
        }

    def get_table_path(self, table_name: TableNames) -> Path:
        """Get the full path for a table."""
        return self.base_path / table_name.value

    def validate_schema(self, df: pd.DataFrame, table_name: TableNames) -> bool:
        """Validate that DataFrame has required columns for table."""
        schema = self.table_schemas[table_name]
        missing_columns = set(schema.required_columns) - set(df.columns)
        if missing_columns:
            self.logger.error(f"Missing required columns for {table_name}: {missing_columns}")
            return False
        return True

    def persist_data(self, df: pd.DataFrame, table_name: TableNames) -> Dict[str, int]:
        """
        Persist data to Delta Lake table with schema validation and proper partitioning.
        Returns statistics about the operation.
        """
        if not self.validate_schema(df, table_name):
            raise ValueError(f"Invalid schema for table {table_name}")

        table_path = str(self.get_table_path(table_name))
        schema = self.table_schemas[table_name]

        try:
            if not (Path(table_path) / '_delta_log').exists():
                # New table creation
                write_deltalake(
                    table_or_uri=table_path,
                    data=df,
                    partition_by=schema.partition_columns,
                    mode='error'  # Fail if table exists
                )
                return {"rows_inserted": len(df), "rows_updated": 0}
            else:
                # Update existing table
                dt = DeltaTable(table_path)
                merger = dt.merge(
                    source=df,
                    predicate="s.news_id = t.news_id",
                    source_alias="s",
                    target_alias="t"
                )
                merger.when_matched_update_all()
                merger.when_not_matched_insert_all()
                
                result = merger.execute()
                
                # Vacuum old files
                dt.vacuum(retention_hours=168)  # 1 week retention
                
                return result

        except Exception as e:
            self.logger.error(f"Error persisting data to {table_name}: {str(e)}")
            raise

    def read_table(
        self,
        table_name: TableNames,
        filters: Optional[List[tuple]] = None,
        columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Read data from Delta Lake table with optional filters and column selection.
        """
        table_path = str(self.get_table_path(table_name))
        
        try:
            dt = DeltaTable(table_path)
            df = dt.to_pandas(filters=filters, columns=columns)
            return df
        except Exception as e:
            self.logger.error(f"Error reading table {table_name}: {str(e)}")
            raise

    def get_latest_news_ids(self, days_back: int = 1) -> List[str]:
        """Get news IDs from recent data."""
        filters = [
            ('date_utc', '>=', (datetime.now() - pd.Timedelta(days=days_back)).strftime('%Y-%m-%d'))
        ]
        df = self.read_table(TableNames.RAW_METADATA, filters=filters, columns=['news_id'])
        return df['news_id'].tolist()
