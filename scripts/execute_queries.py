#!/usr/bin/env python3
"""
PostgreSQL Benchmarking Script
Compares performance of basic queries, materialized views, and incremental materialized views (pg_ivm)
"""

"""
1. connect to PostgreSQL database
2. (?) warmup the database by simple SELECT queries
3. Each select statement is an experiment. For each select statement:
    a. basic setup: no IMVs, no triggers. 
        - execute the select statement and collect execution times and plans.
        - Execute the write statements and collect execution times and plans.
        - rollback the changes.
    c. Materialized View setup: 
        - create materialized view for the select statement
        - execute the select statement and collect execution times and plans.
        - execute the write statements and collect execution times and plans. 
        - Refresh the materialized view after each write statement. Collect execution times.
    d. Incremental View Maintenance setup:
        - create immv for the select statement using the pg_ivm extension,
        - execute the select statement and collect execution times and plans.
        - execute the write statements and collect execution times and trigger plans of the immv.
        - rollback the changes.
4. Save the results in a CSV file.
5. Close the database connection.
"""

import argparse
import os
from typing import List
import pandas as pd
import logging

from classes.postgresql_benchmark import PostgreSQLBenchmark

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def read_sql_file(filename: str) -> List[str]:
    """Read SQL statements from file"""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Split by semicolon and clean
        statements = [stmt.strip() for stmt in content.split(';') if stmt.strip()]
          
        return statements
    except Exception as e:
        logger.error(f"Error reading file '{filename}': {e}", exc_info=True)
        return []

def load_pairs_from_csv(pairs_file: str) -> pd.DataFrame:
    """Load select-write pairs from CSV file"""
    try:
        return pd.read_csv(pairs_file)
    except Exception as e:
        logger.error(f"Error reading pairs file '{pairs_file}': {e}", exc_info=True)
        return pd.DataFrame()

def main():
    parser = argparse.ArgumentParser(description='PostgreSQL Benchmarking Tool')
    parser.add_argument('--select_file', default='/app/data/imv_test_workload.sql', help='File containing SELECT statements')
    parser.add_argument('--write_file', default='/app/data/write_workload.sql', help='File containing WRITE statements')
    parser.add_argument('--pairs_file', default='/app/data/matches.csv', help='CSV file containing pairs of SELECT and WRITE statements')
    parser.add_argument('--output', default='/app/data/benchmark_results.csv', help='Output CSV file name')
    parser.add_argument('--warmup_rounds', type=int, default=3, help='Number of warmup rounds')
    parser.add_argument('--limit_experiments', type=int, help='Limit number of experiments (for testing)')
    
    args = parser.parse_args()
    
    # Database configuration from environment variables
    config = {
        'host': os.getenv('PGHOST', 'localhost'),
        'port': int(os.getenv('PGPORT', 5432)),
        'db_name': os.getenv('PGDATABASE', 'baseball'),
        'user': os.getenv('PGUSER', 'myuser'),
        'password': os.getenv('PGPASSWORD', 'mypassword'),
    }
    
    # Initialize benchmark
    benchmark = PostgreSQLBenchmark(**config)
    
    if not benchmark.connect():
        logger.error("Failed to connect to database. Exiting.")
        return 1
    
    try:
        # Load SQL statements
        select_statements = read_sql_file(args.select_file)
        write_statements = read_sql_file(args.write_file)
        pairs_df = load_pairs_from_csv(args.pairs_file)
        
        if not select_statements or not write_statements or pairs_df.empty:
            logger.error("Failed to load required data files")
            return 1
        
        logger.info(f"Loaded {len(select_statements)} SELECT statements")
        logger.info(f"Loaded {len(write_statements)} WRITE statements")
        logger.info(f"Loaded {len(pairs_df)} statement pairs")
        
        # Warmup database
        benchmark.warmup_database(select_statements, args.warmup_rounds)
        
        # Run experiments
        experiment_count = 0
        for _, pair in pairs_df.iterrows():
            if args.limit_experiments and experiment_count >= args.limit_experiments:
                break
                
            select_id = pair['select_id']
            write_id = pair['write_id']
            
            if select_id < len(select_statements) and write_id < len(write_statements):
                select_stmt = select_statements[select_id]
                write_stmt = write_statements[write_id]
                
                experiment_id = f"exp_{select_id}_{write_id}"
                
                # Run experiment with current pair
                results = benchmark.run_experiment(select_stmt, [write_stmt], experiment_id)
                benchmark.results.extend(results)
                
                experiment_count += 1
                
                if experiment_count % 10 == 0:
                    logger.info(f"Completed {experiment_count} experiments")
            else:
                logger.warning(f"Invalid indices in pair: select_id={select_id}, write_id={write_id}")
        
        # Save results
        benchmark.save_results_to_csv(args.output)
        logger.info(f"Benchmark completed. {experiment_count} experiments processed.")
        
    except Exception as e:
        logger.error(f"Error during benchmark execution: {e}", exc_info=True)
        return 1
    finally:
        benchmark.close_connection()
    
    return 0

if __name__ == "__main__":
    exit(main())