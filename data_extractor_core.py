"""
Core Data Extraction Module
Handles extraction from various database sources with parallel processing and batching
"""

import logging
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Any, Optional, Iterator, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import time
import hashlib

from utils.database_factory import DatabaseFactory
from utils.file_manager import FileManager
from utils.sql_parser import SQLParser
from utils.error_handler import ExtractionError, DatabaseError


@dataclass
class ExtractionConfig:
    """Configuration for a single extraction task."""
    extraction_id: str
    source_name: str
    source_type: str
    target_name: Optional[str]
    target_type: Optional[str]
    stage_name: Optional[str]  # For Snowflake staging
    sql_file_path: Optional[str]
    where_clause: Optional[str]
    exclude_columns: List[str]
    custom_sql: Optional[str]


@dataclass
class ExtractionResult:
    """Result of an extraction operation."""
    extraction_id: str
    status: str  # success, failed, partial
    records_extracted: int
    files_created: List[str]
    execution_time: float
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = None


class DataExtractor:
    """
    High-performance data extraction engine with parallel processing and batching.
    """
    
    def __init__(
        self,
        config: Dict[str, Any],
        batch_size: int = 10000,
        max_workers: int = 4,
        output_format: str = "parquet",
        output_dir: str = "./extracted_data"
    ):
        self.config = config
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.output_format = output_format
        self.output_dir = Path(output_dir)
        
        # Initialize components
        self.db_factory = DatabaseFactory()
        self.file_manager = FileManager(output_format)
        self.sql_parser = SQLParser()
        
        # Performance tracking
        self.extraction_stats = {
            "total_extractions": 0,
            "successful_extractions": 0,
            "total_records": 0,
            "total_files": 0,
            "start_time": None,
            "end_time": None
        }
        
        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        logging.info(f"DataExtractor initialized:")
        logging.info(f"  - Batch size: {batch_size:,}")
        logging.info(f"  - Max workers: {max_workers}")
        logging.info(f"  - Output format: {output_format}")
        logging.info(f"  - Output directory: {output_dir}")

    def extract_from_mapping(self, mapping_file: str) -> List[ExtractionResult]:
        """
        Extract data based on mapping file configuration.
        
        Args:
            mapping_file: Path to CSV mapping file
            
        Returns:
            List of extraction results
        """
        
        self.extraction_stats["start_time"] = time.time()
        
        try:
            # Load and parse mapping file
            mapping_configs = self._load_mapping_file(mapping_file)
            logging.info(f"Loaded {len(mapping_configs)} extraction configurations")
            
            # Execute extractions in parallel
            results = self._execute_parallel_extractions(mapping_configs)
            
            # Update stats
            self.extraction_stats["end_time"] = time.time()
            self.extraction_stats["total_extractions"] = len(results)
            self.extraction_stats["successful_extractions"] = sum(
                1 for r in results if r.status == "success"
            )
            self.extraction_stats["total_records"] = sum(
                r.records_extracted for r in results
            )
            self.extraction_stats["total_files"] = sum(
                len(r.files_created) for r in results
            )
            
            self._log_final_stats()
            
            return results
            
        except Exception as e:
            logging.error(f"Critical error in extraction process: {str(e)}")
            raise ExtractionError(f"Extraction process failed: {str(e)}") from e

    def _load_mapping_file(self, mapping_file: str) -> List[ExtractionConfig]:
        """Load and parse the mapping CSV file."""
        
        try:
            df = pd.read_csv(mapping_file)
            
            # Validate required columns
            required_columns = ["extraction_id", "source_name", "source_type"]
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            configs = []
            
            for _, row in df.iterrows():
                config = ExtractionConfig(
                    extraction_id=str(row["extraction_id"]),
                    source_name=str(row["source_name"]).strip(),
                    source_type=str(row["source_type"]).strip().lower(),
                    target_name=row.get("target_name"),
                    target_type=row.get("target_type"),
                    stage_name=row.get("stage_name"),
                    sql_file_path=row.get("sql_file_path"),
                    where_clause=row.get("where_clause"),
                    exclude_columns=self._parse_exclude_columns(row.get("exclude_columns", "")),
                    custom_sql=row.get("custom_sql")
                )
                
                # Validate configuration
                self._validate_extraction_config(config)
                configs.append(config)
            
            return configs
            
        except Exception as e:
            logging.error(f"Failed to load mapping file: {str(e)}")
            raise ExtractionError(f"Invalid mapping file: {str(e)}") from e

    def _parse_exclude_columns(self, exclude_str: str) -> List[str]:
        """Parse exclude columns string into list."""
        if not exclude_str or pd.isna(exclude_str):
            return []
        
        return [col.strip() for col in str(exclude_str).split(",") if col.strip()]

    def _validate_extraction_config(self, config: ExtractionConfig) -> None:
        """Validate extraction configuration."""
        
        if not config.source_name:
            raise ValueError(f"Empty source_name for extraction {config.extraction_id}")
        
        if config.source_type not in ["table", "query", "file"]:
            raise ValueError(f"Invalid source_type '{config.source_type}' for extraction {config.extraction_id}")
        
        # Validate SQL file path if provided
        if config.sql_file_path and not Path(config.sql_file_path).exists():
            raise FileNotFoundError(f"SQL file not found: {config.sql_file_path}")

    def _execute_parallel_extractions(self, configs: List[ExtractionConfig]) -> List[ExtractionResult]:
        """Execute extractions in parallel with controlled concurrency."""
        
        results = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all extraction tasks
            future_to_config = {
                executor.submit(self._extract_single_source, config): config
                for config in configs
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_config):
                config = future_to_config[future]
                
                try:
                    result = future.result()
                    results.append(result)
                    
                    logging.info(
                        f"✓ Extraction {config.extraction_id} completed: "
                        f"{result.records_extracted:,} records in {result.execution_time:.2f}s"
                    )
                    
                except Exception as e:
                    error_result = ExtractionResult(
                        extraction_id=config.extraction_id,
                        status="failed",
                        records_extracted=0,
                        files_created=[],
                        execution_time=0,
                        error_message=str(e)
                    )
                    results.append(error_result)
                    
                    logging.error(f"✗ Extraction {config.extraction_id} failed: {str(e)}")
        
        return results

    def _extract_single_source(self, config: ExtractionConfig) -> ExtractionResult:
        """Extract data from a single source."""
        
        start_time = time.time()
        files_created = []
        total_records = 0
        
        try:
            logging.info(f"Starting extraction {config.extraction_id}: {config.source_name}")
            
            # Get database connection
            source_conn = self._get_source_connection(config)
            
            # Build query
            query = self._build_extraction_query(config, source_conn)
            logging.debug(f"Extraction query: {query}")
            
            # Get table metadata
            metadata = self._get_table_metadata(source_conn, config.source_name)
            
            # Extract data in batches
            batch_files = []
            batch_num = 0
            
            for batch_df in self._extract_data_batches(source_conn, query, config):
                if batch_df.empty:
                    continue
                
                batch_num += 1
                total_records += len(batch_df)
                
                # Apply column exclusions
                if config.exclude_columns:
                    batch_df = self._exclude_columns(batch_df, config.exclude_columns)
                
                # Generate output file path
                file_path = self._generate_file_path(config, batch_num)
                
                # Save batch
                saved_path = self.file_manager.save_dataframe(
                    batch_df, 
                    file_path, 
                    self.output_format
                )
                
                batch_files.append(str(saved_path))
                
                logging.debug(
                    f"Saved batch {batch_num} for {config.extraction_id}: "
                    f"{len(batch_df):,} records -> {saved_path}"
                )
            
            files_created.extend(batch_files)
            
            # Close connection
            if hasattr(source_conn, 'close'):
                source_conn.close()
            
            execution_time = time.time() - start_time
            
            return ExtractionResult(
                extraction_id=config.extraction_id,
                status="success",
                records_extracted=total_records,
                files_created=files_created,
                execution_time=execution_time,
                metadata=metadata
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            
            logging.error(f"Extraction {config.extraction_id} failed after {execution_time:.2f}s: {str(e)}")
            
            return ExtractionResult(
                extraction_id=config.extraction_id,
                status="failed",
                records_extracted=total_records,
                files_created=files_created,
                execution_time=execution_time,
                error_message=str(e)
            )

    def _get_source_connection(self, config: ExtractionConfig):
        """Get database connection for source."""
        
        source_config = self.config.get("source", {})
        db_type = source_config.get("type")
        
        if not db_type:
            raise ValueError("Source database type not configured")
        
        return self.db_factory.create_connection(db_type, source_config)

    def _build_extraction_query(self, config: ExtractionConfig, connection) -> str:
        """Build the extraction SQL query."""
        
        if config.custom_sql:
            return config.custom_sql
        
        if config.sql_file_path:
            return self.sql_parser.load_sql_file(config.sql_file_path)
        
        # Build basic SELECT query
        query = f"SELECT * FROM {config.source_name}"
        
        if config.where_clause:
            query += f" WHERE {config.where_clause}"
        
        return query

    def _get_table_metadata(self, connection, table_name: str) -> Dict[str, Any]:
        """Get metadata about the source table."""
        
        try:
            # This is a simplified version - you might want to expand this
            # based on your specific database types
            metadata = {
                "table_name": table_name,
                "extraction_timestamp": pd.Timestamp.now().isoformat(),
                "columns": [],
                "row_count": None
            }
            
            # Try to get column info (database-specific implementation needed)
            if hasattr(connection, 'get_table_schema'):
                metadata["columns"] = connection.get_table_schema(table_name)
            
            return metadata
            
        except Exception as e:
            logging.warning(f"Could not retrieve metadata for {table_name}: {str(e)}")
            return {"table_name": table_name, "error": str(e)}

    def _extract_data_batches(self, connection, query: str, config: ExtractionConfig) -> Iterator[pd.DataFrame]:
        """Extract data in batches for memory efficiency."""
        
        try:
            # Use pandas read_sql with chunksize for batching
            if hasattr(connection, 'get_pandas_connection'):
                # For connections that provide pandas-compatible interface
                pandas_conn = connection.get_pandas_connection()
                
                for chunk in pd.read_sql(
                    query, 
                    pandas_conn, 
                    chunksize=self.batch_size
                ):
                    yield chunk
            
            else:
                # For custom connection types, implement batch reading
                offset = 0
                
                while True:
                    batch_query = f"""
                    SELECT * FROM ({query}) AS subquery 
                    LIMIT {self.batch_size} OFFSET {offset}
                    """
                    
                    batch_data = connection.execute_query(batch_query)
                    
                    if not batch_data or len(batch_data) == 0:
                        break
                    
                    # Convert to DataFrame
                    df = pd.DataFrame(batch_data)
                    
                    if df.empty:
                        break
                    
                    yield df
                    
                    if len(df) < self.batch_size:
                        break
                    
                    offset += self.batch_size
                    
        except Exception as e:
            logging.error(f"Error extracting data batches: {str(e)}")
            raise DatabaseError(f"Failed to extract data: {str(e)}") from e

    def _exclude_columns(self, df: pd.DataFrame, exclude_columns: List[str]) -> pd.DataFrame:
        """Remove specified columns from DataFrame."""
        
        existing_exclude = [col for col in exclude_columns if col in df.columns]
        
        if existing_exclude:
            df = df.drop(columns=existing_exclude)
            logging.debug(f"Excluded columns: {existing_exclude}")
        
        return df

    def _generate_file_path(self, config: ExtractionConfig, batch_num: int) -> Path:
        """Generate output file path for extracted data."""
        
        # Parse source name to create directory structure
        # Format: db/schema/table/table_batch_001.parquet
        
        parts = config.source_name.replace("`", "").split(".")
        
        if len(parts) >= 3:
            db_name, schema_name, table_name = parts[0], parts[1], parts[2]
        elif len(parts) == 2:
            db_name, table_name = parts[0], parts[1]
            schema_name = "default"
        else:
            db_name = "default"
            schema_name = "default"
            table_name = parts[0] if parts else "unknown"
        
        # Clean names for file system
        db_name = self._clean_filename(db_name)
        schema_name = self._clean_filename(schema_name)
        table_name = self._clean_filename(table_name)
        
        # Create directory structure
        dir_path = self.output_dir / db_name / schema_name / table_name
        dir_path.mkdir(parents=True, exist_ok=True)
        
        # Generate filename
        extension = "parquet" if self.output_format == "parquet" else "csv"
        filename = f"{table_name}_batch_{batch_num:03d}.{extension}"
        
        return dir_path / filename

    def _clean_filename(self, name: str) -> str:
        """Clean string for use as filename/directory name."""
        
        # Remove or replace invalid characters
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            name = name.replace(char, '_')
        
        # Remove leading/trailing whitespace and dots
        name = name.strip('. ')
        
        # Ensure not empty
        if not name:
            name = "unnamed"
        
        return name

    def _log_final_stats(self) -> None:
        """Log final extraction statistics."""
        
        stats = self.extraction_stats
        
        if stats["start_time"] and stats["end_time"]:
            total_time = stats["end_time"] - stats["start_time"]
            
            logging.info("=" * 50)
            logging.info("EXTRACTION SUMMARY")
            logging.info("=" * 50)
            logging.info(f"Total extractions: {stats['total_extractions']}")
            logging.info(f"Successful: {stats['successful_extractions']}")
            logging.info(f"Failed: {stats['total_extractions'] - stats['successful_extractions']}")
            logging.info(f"Total records extracted: {stats['total_records']:,}")
            logging.info(f"Total files created: {stats['total_files']}")
            logging.info(f"Total execution time: {total_time:.2f} seconds")
            
            if stats['total_records'] > 0 and total_time > 0:
                rate = stats['total_records'] / total_time
                logging.info(f"Average extraction rate: {rate:,.0f} records/second")
            
            logging.info("=" * 50)
