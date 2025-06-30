#!/usr/bin/env python3
"""
Baseball Database Setup Script
This script connects to PostgreSQL, creates a database if needed,
sets up tables from schema file, and imports CSV data.
"""
import sys
import os
import logging

from classes.baseball_db import BaseballDB

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    # DB Configuration
    config = {
        'host':     os.getenv('PGHOST', 'localhost'),
        'port':     int(os.getenv('PGPORT', 5432)),
        'db_name':   os.getenv('PGDATABASE', 'baseball'),
        'user':     os.getenv('PGUSER', 'postgres'),
        'password': os.getenv('PGPASSWORD', 'admin'),
    }
    schema_file = '/app/data/modified_schema.sql'
    csv_directory = '/app/data/norm_tables'
    
    # Create database setup instance
    db_setup = BaseballDB(**config)
    
    try:
        # Step 1: Connect to PostgreSQL and create database if needed
        logger.info("Step 1: Connecting to PostgreSQL server...")
        if not db_setup.connect_to_postgres():
            sys.exit(1)
        
        logger.info("Step 2: Creating database if it doesn't exist...")
        if not db_setup.create_database():
            sys.exit(1)
        
        # Step 3: Connect to target database
        logger.info("Step 3: Connecting to target database...")
        if not db_setup.connect_to_target_db():
            sys.exit(1)
        
        # Step 4: Execute schema file
        logger.info("Step 4: Creating tables from schema file...")
        if not db_setup.execute_schema_file(schema_file):
            sys.exit(1)
        
        # Step 5: Import CSV data
        logger.info("Step 5: Importing CSV data...")
        if not db_setup.import_all_csvs(csv_directory):
            sys.exit(1)
        
        logger.info("Database setup completed successfully!")
        
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        db_setup.close_connection()


if __name__ == "__main__":
    main()