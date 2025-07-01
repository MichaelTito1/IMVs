#!/usr/bin/env python3
"""
Script to filter write statements from workload.csv.
This script reads the workload.csv file and extracts only the write statements
(INSERT, UPDATE, DELETE operations), saving them to a separate file.
"""

import csv
import argparse
import os
from datetime import datetime
import re

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Filter write statements from workload.csv.')
    parser.add_argument('--input', default='/app/data/workload.csv', help='Input workload CSV file')
    parser.add_argument('--output', default='/app/data/write_statements.csv', help='Output file for write statements')
    parser.add_argument('--format', choices=['csv', 'sql'], default='csv', 
                       help='Output format: csv (default) or sql statements only')
    return parser.parse_args()

def remove_table_suffixes(sql_statement):
    """
    Remove _{number} suffixes from table names in SQL statements.
    
    Args:
        sql_statement (str): The SQL statement containing table names with suffixes
        
    Returns:
        str: The SQL statement with suffixes removed from table names
    """
    # Pattern to match table names with suffixes like "table_0", "table_123", etc.
    # This looks for quoted table names followed by _digits
    pattern = r'"([a-zA-Z_][a-zA-Z0-9_]*)_\d+"'
    
    # Replace with just the table name without the suffix
    def replacement(match):
        table_name = match.group(1)  # Get the table name without the suffix
        return f'"{table_name}"'
    
    cleaned_sql = re.sub(pattern, replacement, sql_statement)
    return cleaned_sql

def filter_write_statements(input_file, output_file, output_format='csv'):
    """Filter write statements from the workload CSV file."""
    
    write_types = {'insert', 'update', 'delete'}
    write_statements = []
    
    print(f"Reading workload from: {input_file}")
    
    with open(input_file, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        
        for row in reader:
            query_type = row.get('query_type', '').lower().strip()
            
            if query_type in write_types:
                write_statements.append(row)
    
    print(f"Found {len(write_statements)} write statements:")
    
    # Clean SQL statements and remove table suffixes
    for stmt in write_statements:
        sql = stmt.get('sql', '').strip()
        if sql:
            # Remove table suffixes
            cleaned_sql = remove_table_suffixes(sql)
            stmt['sql'] = cleaned_sql

    # Count by type
    type_counts = {}
    for stmt in write_statements:
        query_type = stmt['query_type'].lower()
        type_counts[query_type] = type_counts.get(query_type, 0) + 1
    
    for stmt_type, count in type_counts.items():
        print(f"  {stmt_type.upper()}: {count}")
    
    # Write output
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
        
        # Write header
        writer.writeheader()
        
        # Write filtered rows
        for row in write_statements:
            writer.writerow(row)

def write_sql_output(write_statements, output_file):
    """Write only the SQL statements to a text file."""
    with open(output_file, 'w', encoding='utf-8') as f:
        # f.write(f"-- Write statements extracted from workload.csv\n")
        # f.write(f"-- Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        # f.write(f"-- Total statements: {len(write_statements)}\n\n")
        
        current_type = None
        for i, stmt in enumerate(write_statements):
            query_type = stmt['query_type'].upper()
            sql = stmt.get('sql', '').strip()
            
            if current_type != query_type:
                # f.write(f"\n-- {query_type} STATEMENTS\n")
                current_type = query_type
            
            # f.write(f"-- Statement {i+1}: Query ID {stmt.get('query_id', 'N/A')}\n")
            f.write(f"{sql}\n")

def main():
    args = parse_args()
    
    # Convert relative paths to absolute paths
    input_file = os.path.abspath(args.input)
    output_file = os.path.abspath(args.output)
    
    # Verify input file exists
    if not os.path.exists(input_file):
        print(f"Error: Input file {input_file} not found")
        return 1
    
    # Create output directory if needed
    output_dir = os.path.dirname(output_file)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    try:
        filter_write_statements(input_file, output_file, args.format)
        return 0
        
    except Exception as e:
        print(f"Error: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
