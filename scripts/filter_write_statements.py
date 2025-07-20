#!/usr/bin/env python3
"""
Script to filter write statements from workload.csv.
This script reads the workload.csv file and extracts only the write statements
(INSERT, UPDATE, DELETE operations), removes duplicate SQL statements,
saving them to a separate file.
"""

import csv
import argparse
import os
import re
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
MAX_JOINS_PER_QUERY = 3

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Filter write statements from workload.csv.')
    parser.add_argument('--input', default='/app/data/workload.csv', help='Input workload CSV file')
    parser.add_argument('--output', default='/app/data/write_statements.csv', help='Output file for write statements')
    parser.add_argument('--format', choices=['csv', 'sql'], default='csv', 
                       help='Output format: csv (default) or sql statements only')
    parser.add_argument('--max-joins-per-query', type=int, default=MAX_JOINS_PER_QUERY,
                       help='Maximum number of joins per query')
    
    return parser.parse_args()

def remove_table_suffixes(sql: str) -> str:
    """
    Remove _{number} suffixes from table names (quoted or unquoted)
    in SQL statements.
    """
    pattern = re.compile(
        r'''(?P<quote>\")?
            (?P<name>[A-Za-z_][A-Za-z0-9_]*?)
            _\d+
            (?P=quote)?
        ''',
        flags=re.VERBOSE
    )
    return pattern.sub(lambda m: f'{m.group("quote") or ""}{m.group("name")}{m.group("quote") or ""}',
                       sql)

def filter_write_statements(input_file, output_file, output_format='csv', max_joins_per_query=MAX_JOINS_PER_QUERY):
    """Filter write statements from the workload CSV file and remove duplicates."""
    write_types = {'insert', 'update', 'delete'}
    write_statements = []
    
    print(f"Reading workload from: {input_file}")
    with open(input_file, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            query_type = row.get('query_type', '').lower().strip()
            if query_type not in write_types:
                continue
            num_joins = len(row.get('join_tables').split(',') if row.get('join_tables') else [])
            if num_joins > max_joins_per_query:
                print(f"Skipping query with {num_joins} joins (max allowed: {max_joins_per_query})")
                continue

            # TODO: TEMPORARY: Skip INSERT statements
            sql = row.get('sql', '').strip()
            if sql.startswith('INSERT INTO'):
                logger.info(f"Found INSERT statement: {sql}. Skipping...")
                continue
            write_statements.append(row)
    
    print(f"Found {len(write_statements)} write statements (including duplicates) out of {reader.line_num} total rows.")
    # Clean SQL and remove table suffixes
    for stmt in write_statements:
        sql = stmt.get('sql', '').strip()
        if sql:
            stmt['sql'] = remove_table_suffixes(sql)
    
    # Deduplicate by cleaned SQL
    unique = []
    seen_sql = set()
    for stmt in write_statements:
        sql = stmt.get('sql', '')
        if sql and sql not in seen_sql:
            unique.append(stmt)
            seen_sql.add(sql)
    duplicates = len(write_statements) - len(unique)
    if duplicates:
        print(f"Removed {duplicates} duplicate statements")
    write_statements = unique
    print(f"Proceeding with {len(write_statements)} unique write statements")
    
    # Count by type
    type_counts = {}
    for stmt in write_statements:
        t = stmt['query_type'].lower()
        type_counts[t] = type_counts.get(t, 0) + 1
    for stmt_type, count in type_counts.items():
        print(f"  {stmt_type.upper()}: {count}")
    
    # Write out
    if output_format == 'csv':
        write_csv_output(write_statements, output_file)
    else:
        write_sql_output(write_statements, output_file)
    print(f"Write statements saved to: {output_file}")

def write_csv_output(write_statements, output_file):
    """Write filtered statements to CSV format."""
    if not write_statements:
        print("No write statements found.")
        return
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = write_statements[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in write_statements:
            writer.writerow(row)

def write_sql_output(write_statements, output_file):
    """Write only the SQL statements to a text file."""
    with open(output_file, 'w', encoding='utf-8') as f:
        current_type = None
        for stmt in write_statements:
            query_type = stmt['query_type'].upper()
            sql = stmt.get('sql', '').strip()
            f.write(f"{sql}\n")

def main():
    args = parse_args()
    input_file = os.path.abspath(args.input)
    output_file = os.path.abspath(args.output)
    if not os.path.exists(input_file):
        print(f"Error: Input file {input_file} not found")
        return 1
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    max_joins_per_query = args.max_joins_per_query
    if max_joins_per_query <= 0:
        logger.error("--max-joins-per-query must be a positive integer!")
        return 1
    try:
        filter_write_statements(input_file, output_file, args.format, max_joins_per_query)
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
