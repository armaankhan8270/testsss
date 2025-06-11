"""
Utility Modules for Data Extraction System
Contains supporting components for configuration, logging, file management, etc.
"""

# =============================================================================
# utils/config_manager.py
# =============================================================================

import os
import yaml
import logging
from pathlib import Path
from typing import Dict, Any


class ConfigManager:
    """Manages configuration files for the extraction system."""
    
    def __init__(self, config_dir: Path):
        self.config_dir = Path(config_dir)
        self.config_dir.mkdir(parents=True, exist_ok=True)
    
    def save_config(self, config: Dict[str, Any], project_name: str) -> None:
        """Save configuration to YAML file."""
        
        config_path = self.config_dir / f"{project_name}.yaml"
        
        try:
            with open(config_path, "w", encoding="utf-8") as file:
                yaml.dump({project_name: config}, file, default_flow_style=False, indent=2)
            
            logging.info(f"Configuration saved: {config_path}")
            
        except Exception as e:
            logging.error(f"Failed to save configuration: {str(e)}")
            raise
    
    def load_config(self, project_name: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        
        config_path = self.config_dir / f"{project_name}.yaml"
        
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        try:
            with open(config_path, "r", encoding="utf-8") as file:
                config_data = yaml.safe_load(file)
            
            project_config = config_data.get(project_name, {})
            
            if not project_config:
                raise ValueError(f"No configuration found for project: {project_name}")
            
            logging.info(f"Configuration loaded: {config_path}")
            return project_config
            
        except Exception as e:
            logging.error(f"Failed to load configuration: {str(e)}")
            raise


# =============================================================================
# utils/logging_config.py
# =============================================================================

import logging
import os
from datetime import datetime
from pathlib import Path


def setup_logging(log_prefix: str = "extraction") -> str:
    """
    Setup logging configuration with timestamped log files.
    
    Args:
        log_prefix: Prefix for log filename
        
    Returns:
        Path to created log file
    """
    
    # Create logging directory
    log_dir = Path("logging")
    log_dir.mkdir(exist_ok=True)
    
    # Create timestamped log filename
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_filename = log_dir / f"{log_prefix}_{timestamp}.log"
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_filename, encoding="utf-8"),
            logging.StreamHandler()  # Also log to console
        ]
    )
    
    # Set third-party library log levels
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("pandas").setLevel(logging.WARNING)
    
    # Log startup message
    logging.info(f"Logging initialized - Log file: {log_filename}")
    
    return str(log_filename)


# =============================================================================
# utils/file_manager.py
# =============================================================================

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from typing import Union, Optional
import logging


class FileManager:
    """Handles file operations for extracted data."""
    
    def __init__(self, default_format: str = "parquet"):
        self.default_format = default_format.lower()
        
        if self.default_format not in ["csv", "parquet"]:
            raise ValueError("Supported formats: csv, parquet")
    
    def save_dataframe(
        self, 
        df: pd.DataFrame, 
        file_path: Union[str, Path], 
        format_type: Optional[str] = None
    ) -> Path:
        """
        Save DataFrame to file with optimized settings.
        
        Args:
            df: DataFrame to save
            file_path: Output file path
            format_type: File format (csv/parquet), uses default if None
            
        Returns:
            Path to saved file
        """
        
        file_path = Path(file_path)
        format_type = format_type or self.default_format
        
        # Ensure directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            if format_type == "parquet":
                return self._save_parquet(df, file_path)
            elif format_type == "csv":
                return self._save_csv(df, file_path)
            else:
                raise ValueError(f"Unsupported format: {format_type}")
                
        except Exception as e:
            logging.error(f"Failed to save file {file_path}: {str(e)}")
            raise
    
    def _save_parquet(self, df: pd.DataFrame, file_path: Path) -> Path:
        """Save DataFrame as Parquet with compression."""
        
        # Optimize data types before saving
        df_optimized = self._optimize_dtypes(df)
        
        # Save with compression
        df_optimized.to_parquet(
            file_path,
            engine="pyarrow",
            compression="snappy",
            index=False
        )
        
        logging.debug(f"Saved Parquet file: {file_path} ({len(df):,} rows)")
        return file_path
    
    def _save_csv(self, df: pd.DataFrame, file_path: Path) -> Path:
        """Save DataFrame as CSV with proper encoding and escaping."""
        
        df.to_csv(
            file_path,
            index=False,
            encoding="utf-8",
            quoting=1,  # Quote all fields
            escapechar="\\",
            doublequote=True
        )
        
        logging.debug(f"Saved CSV file: {file_path} ({len(df):,} rows)")
        return file_path
    
    def _optimize_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimize DataFrame data types for better compression."""
        
        df_copy = df.copy()
        
        for col in df_copy.columns:
            col_type = df_copy[col].dtype
            
            # Optimize integers
            if col_type in ["int64"]:
                if df_copy[col].min() >= 0:
                    if df_copy[col].max() < 2**32:
                        df_copy[col] = df_copy[col].astype("uint32")
                    else:
                        df_copy[col] = df_copy[col].astype("uint64")
                else:
                    if df_copy[col].min() >= -2**31 and df_copy[col].max() < 2**31:
                        df_copy[col] = df_copy[col].astype("int32")
            
            # Optimize floats
            elif col_type == "float64":
                if df_copy[col].min() >= -3.4e38 and df_copy[col].max() <= 3.4e38:
                    df_copy[col] = df_copy[col].astype("float32")
            
            # Optimize strings/objects
            elif col_type == "object":
                # Try to convert to category if many repeated values
                unique_ratio = df_copy[col].nunique() / len(df_copy)
                if unique_ratio < 0.5:  # Less than 50% unique values
                    try:
                        df_copy[col] = df_copy[col].astype("category")
                    except:
                        pass  # Keep as object if conversion fails
        
        return df_copy


# =============================================================================
# utils/sql_parser.py
# =============================================================================

import re
from pathlib import Path
from typing import Dict, Any, Optional
import logging


class SQLParser:
    """Handles SQL query parsing and templating."""
    
    def __init__(self):
        self.template_pattern = re.compile(r'\{(\w+)\}')
    
    def load_sql_file(self, file_path: str) -> str:
        """Load SQL content from file."""
        
        sql_path = Path(file_path)
        
        if not sql_path.exists():
            raise FileNotFoundError(f"SQL file not found: {file_path}")
        
        try:
            with open(sql_path, "r", encoding="utf-8") as file:
                sql_content = file.read().strip()
            
            logging.debug(f"Loaded SQL file: {file_path}")
            return sql_content
            
        except Exception as e:
            logging.error(f"Failed to load SQL file {file_path}: {str(e)}")
            raise
    
    def apply_template(self, sql_template: str, variables: Dict[str, Any]) -> str:
        """Apply template variables to SQL query."""
        
        try:
            # Find all template variables
            template_vars = self.template_pattern.findall(sql_template)
            
            # Check for missing variables
            missing_vars = [var for var in template_vars if var not in variables]
            if missing_vars:
                raise ValueError(f"Missing template variables: {missing_vars}")
            
            # Apply substitutions
            result_sql = sql_template
            for var_name, var_value in variables.items():
                placeholder = f"{{{var_name}}}"
                result_sql = result_sql.replace(placeholder, str(var_value))
            
            logging.debug(f"Applied template variables: {list(variables.keys())}")
            return result_sql
            
        except Exception as e:
            logging.error(f"Failed to apply SQL template: {str(e)}")
            raise
    
    def validate_sql(self, sql: str) -> bool:
        """Basic SQL validation."""
        
        # Remove comments and normalize whitespace
        cleaned_sql = re.sub(r'--.*, '', sql, flags=re.MULTILINE)
        cleaned_sql = re.sub(r'/\*.*?\*/', '', cleaned_sql, flags=re.DOTALL)
        cleaned_sql = ' '.join(cleaned_sql.split())
        
        if not cleaned_sql:
            return False
        
        # Check for basic SQL structure
        sql_keywords = ['SELECT', 'FROM', 'INSERT', 'UPDATE', 'DELETE', 'WITH']
        has_sql_keyword = any(keyword in cleaned_sql.upper() for keyword in sql_keywords)
        
        return has_sql_keyword


# =============================================================================
# utils/error_handler.py
# =============================================================================

class ExtractionError(Exception):
    """Base exception for extraction operations."""
    pass


class DatabaseError(ExtractionError):
    """Exception for database-related errors."""
    pass


class ConfigurationError(ExtractionError):
    """Exception for configuration-related errors."""
    pass


class FileOperationError(ExtractionError):
    """Exception for file operation errors."""
    pass


# =============================================================================
# utils/database_factory.py
# =============================================================================

import logging
from typing import Dict, Any, Union
from abc import ABC, abstractmethod


class DatabaseConnection(ABC):
    """Abstract base class for database connections."""
    
    @abstractmethod
    def connect(self) -> None:
        """Establish database connection."""
        pass
    
    @abstractmethod
    def execute_query(self, query: str) -> Any:
        """Execute SQL query and return results."""
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close database connection."""
        pass
    
    def get_pandas_connection(self):
        """Return pandas-compatible connection if available."""
        return None


class BigQueryConnection(DatabaseConnection):
    """BigQuery database connection wrapper."""
    
    def __init__(self, project_id: str, credentials_path: str):
        self.project_id = project_id
        self.credentials_path = credentials_path
        self.connection = None
    
    def connect(self) -> None:
        """Establish BigQuery connection."""
        try:
            from google.cloud import bigquery
            from google.oauth2 import service_account
            
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path
            )
            
            self.connection = bigquery.Client(
                project=self.project_id,
                credentials=credentials
            )
            
            logging.info(f"Connected to BigQuery project: {self.project_id}")
            
        except Exception as e:
            raise DatabaseError(f"Failed to connect to BigQuery: {str(e)}")
    
    def execute_query(self, query: str) -> Any:
        """Execute BigQuery SQL."""
        if not self.connection:
            self.connect()
        
        try:
            query_job = self.connection.query(query)
            return query_job.result()
            
        except Exception as e:
            raise DatabaseError(f"BigQuery query failed: {str(e)}")
    
    def get_pandas_connection(self):
        """Return pandas-compatible connection."""
        if not self.connection:
            self.connect()
        return self.connection
    
    def close(self) -> None:
        """Close BigQuery connection."""
        if self.connection:
            self.connection.close()
            self.connection = None


class SQLServerConnection(DatabaseConnection):
    """SQL Server database connection wrapper."""
    
    def __init__(self, server: str, database: str, username: str, password: str):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.connection = None
    
    def connect(self) -> None:
        """Establish SQL Server connection."""
        try:
            import pyodbc
            
            connection_string = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"UID={self.username};"
                f"PWD={self.password};"
                f"TrustServerCertificate=yes;"
            )
            
            self.connection = pyodbc.connect(connection_string)
            logging.info(f"Connected to SQL Server: {self.server}/{self.database}")
            
        except Exception as e:
            raise DatabaseError(f"Failed to connect to SQL Server: {str(e)}")
    
    def execute_query(self, query: str) -> Any:
        """Execute SQL Server query."""
        if not self.connection:
            self.connect()
        
        try:
            import pandas as pd
            return pd.read_sql(query, self.connection)
            
        except Exception as e:
            raise DatabaseError(f"SQL Server query failed: {str(e)}")
    
    def get_pandas_connection(self):
        """Return pandas-compatible connection."""
        if not self.connection:
            self.connect()
        return self.connection
    
    def close(self) -> None:
        """Close SQL Server connection."""
        if self.connection:
            self.connection.close()
            self.connection = None


class DatabaseFactory:
    """Factory class for creating database connections."""
    
    @staticmethod
    def create_connection(db_type: str, config: Dict[str, Any]) -> DatabaseConnection:
        """Create database connection based on type and configuration."""
        
        db_type = db_type.lower()
        
        if db_type == "bigquery":
            return BigQueryConnection(
                project_id=config["project_id"],
                credentials_path=config["credentials_path"]
            )
        
        elif db_type == "sqlserver":
            return SQLServerConnection(
                server=config["server"],
                database=config["database"],
                username=config["username"],
                password=config["password"]
            )
        
        # Add more database types as needed
        # elif db_type == "oracle":
        #     return OracleConnection(config)
        
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
