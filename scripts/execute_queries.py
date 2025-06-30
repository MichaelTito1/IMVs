import argparse
import os
from typing import Iterator, Tuple
import pandas as pd

from classes.baseball_db import BaseballDB

def read_sql_statements(filename: str) -> Iterator[Tuple[int, str]]:
    """Generator that yields (index, statement) tuples to save memory."""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            content = f.read()
        
        if ';' in content:
            statements = [stmt.strip() for stmt in content.split(';') if stmt.strip()]
        else:
            statements = [line.strip() for line in content.split('\n') if line.strip()]
            
        for idx, stmt in enumerate(statements):
            yield idx, stmt
            
    except Exception as e:
        print(f"Error reading file '{filename}': {e}")
        return
    
def load_data(select_file: str, write_file: str, pairs_file: str) -> None:
    """Load data from SQL files and store them in pandas DataFrames."""

    # Read SQL statements
    select_statements = list(read_sql_statements(select_file))
    write_statements = list(read_sql_statements(write_file))
    
    # Create DataFrames
    select_df = pd.DataFrame(select_statements, columns=['index', 'select_statement'])
    write_df = pd.DataFrame(write_statements, columns=['index', 'write_statement'])
    
    # Save to CSV files
    select_df.to_csv('select_statements.csv', index=False)
    write_df.to_csv('write_statements.csv', index=False)
    
    # Read pairs file
    pairs_df = pd.read_csv(pairs_file)
    
    print("Data loaded and saved to CSV files.")
    return select_df, write_df, pairs_df

def main():
    parser = argparse.ArgumentParser(description='Execute Read/Write SQL statements in a PostgreSQL database, collect and save the execution times and execution plans.')
    parser.add_argument('--select_file', required=True, help='File containing SELECT statements')
    parser.add_argument('--write_file', required=True, help='File containing INSERT/UPDATE/DELETE statements')
    parser.add_argument('--pairs_file', required=True, help='File containing pairs of SELECT and WRITE statements')

    args = parser.parse_args()

    select_df, write_df, pairs_df = load_data(args.select_file, args.write_file, args.pairs_file)

    # group pairs by select_id
    pairs_df = pairs_df.groupby('select_id')

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

    config = {
        'host':     os.getenv('PGHOST', 'localhost'),
        'port':     int(os.getenv('PGPORT', 5432)),
        'dbname':   os.getenv('PGDATABASE', 'baseball'),
        'user':     os.getenv('PGUSER', 'postgres'),
        'password': os.getenv('PGPASSWORD', 'admin'),
    }
    baseball_db = BaseballDB(**config)

    if not baseball_db.connect_to_postgres():
        print("Failed to connect to PostgreSQL. Exiting.")
        return
    
    # Warmup phase: Execute simple SELECT queries
    print("Warming up the database with simple SELECT queries...")
    for idx, row in select_df.iterrows():
        try:
            baseball_db.cursor.execute(row['select_statement'])
            print(f"Warmup SELECT {idx}: executed successfully.")
        except Exception as e:
            print(f"Warmup SELECT {idx}: failed with error: {e}")
    print("Warmup phase completed.")

    # Experiment phase
    print("Starting experiment phase...")
    