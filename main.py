#!/usr/bin/env python3
"""
Employee Extract Batch Job - Main Entry Point

This module serves as the entry point for the employee data extraction batch job.
It orchestrates the ETL pipeline: extracting employee records from DB2,
transforming/filtering the data, and writing formatted output files.

Usage:
    python main.py

Environment Variables (loaded from .env):
    DB2_HOST: Database host address
    DB2_PORT: Database port number
    DB2_DATABASE: Database name
    DB2_SSID: DB2 subsystem identifier
    DB2_USER: Database username
    DB2_PASSWORD: Database password
    OUTPUT_FILE_PATH: Path for output file generation
    LOG_LEVEL: Logging level (DEBUG, INFO, WARNING, ERROR)
"""

import logging
import sys
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

from config.settings import AppConfig
from etl.extract import EmployeeExtractor
from etl.transform import EmployeeTransformer
from reporting.write_outputs import EmployeeFileWriter


def setup_logging(log_level: str) -> None:
    """
    Configure structured logging for the batch job.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def run_batch_job() -> int:
    """
    Execute the employee extraction batch job.

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("="*60)
        logger.info("Employee Extract Batch Job - Starting")
        logger.info("="*60)
        
        # Load configuration
        config = AppConfig()
        logger.info(f"Environment: {config.environment}")
        
        # Step 1: Extract employee records from DB2
        logger.info("Step 1: Extracting employee records from DB2")
        extractor = EmployeeExtractor(config.db_config)
        employee_data = extractor.extract_employees()
        
        initial_count = len(employee_data)
        logger.info(f"Extracted {initial_count} employee records from DB2")
        
        if initial_count == 0:
            logger.warning("No employee records extracted. Job will complete with empty output.")
        
        # Step 2: Transform and filter employee data
        logger.info("Step 2: Transforming and filtering employee data")
        transformer = EmployeeTransformer()
        transformed_data = transformer.transform(employee_data)
        
        final_count = len(transformed_data)
        logger.info(f"Filtered to {final_count} valid employee records")
        
        if initial_count > 0:
            filter_pct = ((initial_count - final_count) / initial_count) * 100
            logger.info(f"Filtered out {initial_count - final_count} records ({filter_pct:.2f}%)")
        
        # Step 3: Sort employee data by EMPID
        logger.info("Step 3: Sorting employee data by EMPID")
        sorted_data = transformer.sort_by_empid(transformed_data)
        logger.info(f"Sorted {len(sorted_data)} employee records")
        
        # Step 4: Write formatted output file
        logger.info("Step 4: Writing formatted output file")
        writer = EmployeeFileWriter(config.output_config)
        output_path = writer.write_employee_file(sorted_data)
        
        logger.info(f"Output file written: {output_path}")
        logger.info(f"Total records written: {len(sorted_data)}")
        
        logger.info("="*60)
        logger.info("Employee Extract Batch Job - Completed Successfully")
        logger.info("="*60)
        
        return 0
        
    except Exception as e:
        logger.error(f"Batch job failed with error: {str(e)}", exc_info=True)
        logger.error("="*60)
        logger.error("Employee Extract Batch Job - FAILED")
        logger.error("="*60)
        return 1


def main() -> int:
    """
    Main function to initialize and run the batch job.

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    # Load environment variables from .env file
    load_dotenv()
    
    # Get configuration for logging
    import os
    log_level = os.getenv('LOG_LEVEL', 'INFO')
    
    # Setup logging
    setup_logging(log_level)
    
    # Run batch job
    exit_code = run_batch_job()
    
    # Exit with appropriate code
    sys.exit(exit_code)


if __name__ == '__main__':
    main()
