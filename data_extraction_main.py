#!/usr/bin/env python3
"""
Dynamic Data Extraction System
Extracts data from various sources (BigQuery, SQL Server, Oracle) and uploads to Snowflake
"""

import logging
import os
import time
import click
import yaml
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

from extractors.data_extractor import DataExtractor
from utils.config_manager import ConfigManager
from utils.logging_config import setup_logging


# Constants
CONFIG_DIR = Path.home() / ".polarsled"
DEFAULT_BATCH_SIZE = 10000
DEFAULT_MAX_WORKERS = 4
SUPPORTED_FORMATS = ["csv", "parquet"]
SUPPORTED_DATABASES = ["bigquery", "sqlserver", "oracle", "snowflake"]


@click.command()
@click.option(
    "--setup", 
    is_flag=True, 
    help="Run the setup wizard to collect connection details."
)
@click.option(
    "--project", 
    type=str, 
    help="Project name to load the configuration."
)
@click.option(
    "--mapping-file", 
    type=str, 
    required=False,
    help="Path to the extraction mapping CSV file."
)
@click.option(
    "--output-format", 
    type=click.Choice(SUPPORTED_FORMATS), 
    default="parquet",
    help="Output format for extracted data (csv/parquet)."
)
@click.option(
    "--batch-size", 
    type=int, 
    default=DEFAULT_BATCH_SIZE,
    help=f"Number of rows per batch (default: {DEFAULT_BATCH_SIZE})."
)
@click.option(
    "--max-workers", 
    type=int, 
    default=DEFAULT_MAX_WORKERS,
    help=f"Maximum number of parallel workers (default: {DEFAULT_MAX_WORKERS})."
)
@click.option(
    "--output-dir", 
    type=str, 
    default="./extracted_data",
    help="Base directory for extracted data output."
)
def polarsled_extract(
    setup: bool,
    project: Optional[str],
    mapping_file: Optional[str],
    output_format: str,
    batch_size: int,
    max_workers: int,
    output_dir: str
):
    """
    PolarSled Data Extraction Tool
    
    Extract data from various sources and optionally upload to target systems.
    """
    
    if setup:
        _run_setup_wizard()
        return
    
    if not project:
        click.echo("❌ Error: Please provide either --setup or --project option.")
        return
    
    if not mapping_file:
        click.echo("❌ Error: Mapping file is required for extraction.")
        return
    
    try:
        # Setup logging
        log_file = setup_logging("data_extraction")
        logging.info("=== PolarSled Data Extraction Started ===")
        click.echo("=== PolarSled Data Extraction Started ===")
        
        # Load configuration
        config_manager = ConfigManager(CONFIG_DIR)
        config = config_manager.load_config(project)
        
        # Validate inputs
        _validate_inputs(mapping_file, output_dir, batch_size, max_workers)
        
        # Run extraction
        _run_extraction(
            config=config,
            mapping_file=mapping_file,
            output_format=output_format,
            batch_size=batch_size,
            max_workers=max_workers,
            output_dir=output_dir
        )
        
        logging.info("=== Data Extraction Completed Successfully ===")
        click.echo("✅ Data extraction completed successfully!")
        click.echo(f"📋 Check logs: {log_file}")
        
    except Exception as e:
        logging.error(f"❌ Extraction failed: {str(e)}")
        click.echo(f"❌ Extraction failed: {str(e)}")
        raise


def _run_setup_wizard() -> None:
    """Run interactive setup wizard to collect database credentials."""
    
    click.echo("🚀 PolarSled Extraction Setup Wizard")
    click.echo("=" * 50)
    
    project_name = click.prompt("Enter project name", type=str)
    
    config = {}
    
    # Source database configuration
    click.echo("\n📥 Source Database Configuration:")
    source_db_type = click.prompt(
        "Source database type",
        type=click.Choice(SUPPORTED_DATABASES),
        default="bigquery"
    )
    
    config["source"] = _collect_db_config(source_db_type, "source")
    
    # Target database configuration (optional)
    has_target = click.confirm("\n📤 Do you want to configure a target database for upload?")
    
    if has_target:
        click.echo("\n📤 Target Database Configuration:")
        target_db_type = click.prompt(
            "Target database type",
            type=click.Choice(SUPPORTED_DATABASES),
            default="snowflake"
        )
        
        config["target"] = _collect_db_config(target_db_type, "target")
    
    # Performance settings
    click.echo("\n⚡ Performance Configuration:")
    config["performance"] = {
        "batch_size": click.prompt("Batch size", type=int, default=DEFAULT_BATCH_SIZE),
        "max_workers": click.prompt("Max parallel workers", type=int, default=DEFAULT_MAX_WORKERS)
    }
    
    # Save configuration
    config_manager = ConfigManager(CONFIG_DIR)
    config_manager.save_config(config, project_name)
    
    click.echo(f"\n✅ Configuration saved successfully!")
    click.echo(f"📁 Config location: {CONFIG_DIR / f'{project_name}.yaml'}")


def _collect_db_config(db_type: str, prefix: str) -> Dict[str, Any]:
    """Collect database-specific configuration."""
    
    config = {"type": db_type}
    
    if db_type == "bigquery":
        config.update({
            "project_id": click.prompt(f"BigQuery project ID"),
            "credentials_path": click.prompt(f"Path to BigQuery credentials JSON file")
        })
    
    elif db_type == "sqlserver":
        config.update({
            "server": click.prompt(f"SQL Server hostname/IP"),
            "database": click.prompt(f"Database name"),
            "username": click.prompt(f"Username"),
            "password": click.prompt(f"Password", hide_input=True)
        })
    
    elif db_type == "oracle":
        config.update({
            "host": click.prompt(f"Oracle host"),
            "port": click.prompt(f"Oracle port", type=int, default=1521),
            "service_name": click.prompt(f"Service name"),
            "username": click.prompt(f"Username"),
            "password": click.prompt(f"Password", hide_input=True)
        })
    
    elif db_type == "snowflake":
        config.update({
            "account": click.prompt(f"Snowflake account"),
            "user": click.prompt(f"Snowflake user"),
            "warehouse": click.prompt(f"Snowflake warehouse"),
            "database": click.prompt(f"Snowflake database"),
            "schema": click.prompt(f"Snowflake schema", default="PUBLIC"),
            "role": click.prompt(f"Snowflake role", default="ACCOUNTADMIN")
        })
        
        # Authentication method
        auth_method = click.prompt(
            "Authentication method",
            type=click.Choice(["password", "private_key"]),
            default="password"
        )
        
        if auth_method == "password":
            config["password"] = click.prompt("Password", hide_input=True)
        else:
            config["private_key_path"] = click.prompt("Path to private key file")
    
    return config


def _validate_inputs(
    mapping_file: str,
    output_dir: str,
    batch_size: int,
    max_workers: int
) -> None:
    """Validate input parameters."""
    
    # Check mapping file exists
    if not Path(mapping_file).exists():
        raise FileNotFoundError(f"Mapping file not found: {mapping_file}")
    
    # Validate batch size
    if batch_size <= 0:
        raise ValueError("Batch size must be greater than 0")
    
    # Validate max workers
    if max_workers <= 0 or max_workers > 32:
        raise ValueError("Max workers must be between 1 and 32")
    
    # Create output directory if it doesn't exist
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    logging.info(f"✓ Input validation passed")
    logging.info(f"  - Mapping file: {mapping_file}")
    logging.info(f"  - Output directory: {output_dir}")
    logging.info(f"  - Batch size: {batch_size}")
    logging.info(f"  - Max workers: {max_workers}")


def _run_extraction(
    config: Dict[str, Any],
    mapping_file: str,
    output_format: str,
    batch_size: int,
    max_workers: int,
    output_dir: str
) -> None:
    """Execute the data extraction process."""
    
    start_time = time.time()
    
    try:
        # Initialize extractor
        extractor = DataExtractor(
            config=config,
            batch_size=batch_size,
            max_workers=max_workers,
            output_format=output_format,
            output_dir=output_dir
        )
        
        # Run extraction
        results = extractor.extract_from_mapping(mapping_file)
        
        # Log summary
        total_records = sum(result.get("records_extracted", 0) for result in results)
        successful_extractions = sum(1 for result in results if result.get("status") == "success")
        
        logging.info(f"📊 Extraction Summary:")
        logging.info(f"  - Total extractions: {len(results)}")
        logging.info(f"  - Successful: {successful_extractions}")
        logging.info(f"  - Failed: {len(results) - successful_extractions}")
        logging.info(f"  - Total records: {total_records:,}")
        
        elapsed_time = time.time() - start_time
        logging.info(f"⏱️ Total extraction time: {elapsed_time:.2f} seconds")
        
        # Print summary to console
        click.echo(f"\n📊 Extraction Summary:")
        click.echo(f"  - Total extractions: {len(results)}")
        click.echo(f"  - Successful: {successful_extractions}")
        click.echo(f"  - Total records: {total_records:,}")
        click.echo(f"  - Time taken: {elapsed_time:.2f} seconds")
        
    except Exception as e:
        logging.error(f"Extraction failed: {str(e)}")
        raise


if __name__ == "__main__":
    polarsled_extract()
