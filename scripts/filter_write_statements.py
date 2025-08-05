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
from utils import remove_table_suffixes

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Filter write statements from workload.csv.')
    parser.add_argument('--input', default='/app/data/write_workload.csv', help='Input workload CSV file')
    parser.add_argument('--output', default='/app/data/write_statements.csv', help='Output file for write statements')
    parser.add_argument('--format', choices=['csv', 'sql'], default='csv', 
                       help='Output format: csv (default) or sql statements only')
    return parser.parse_args()

def add_suffix_to_sql(sql_statement, suffix="RETURNING 1"):
    """
    Add 'RETURNING 1' clause to the end of each SQL write statement.
    """

    sql = sql_statement.strip()
    if not sql:
        return sql
    
    if sql.endswith(';'):
        sql = sql[:-1] + f" {suffix};"
    else:
        sql += f" {suffix};"

    return sql

def filter_write_statements(input_file, output_file, output_format='csv'):
    """Filter write statements from the workload CSV file and remove duplicates."""
    write_statements = []
    
    print(f"Reading workload from: {input_file}")
    with open(input_file, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            write_statements.append(row)
    
    print(f"Found {len(write_statements)} write statements (including duplicates)")
    # Clean SQL and remove table suffixes
    for stmt in write_statements:
        sql = stmt.get('sql', '').strip()
        if sql:
            cleaned_sql = remove_table_suffixes(sql)
            stmt['sql'] = add_suffix_to_sql(cleaned_sql)
    
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
        # Add write_idx as the first field, then the original fieldnames
        original_fieldnames = list(write_statements[0].keys())
        fieldnames = ['write_idx'] + original_fieldnames
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for idx, row in enumerate(write_statements):
            # Create a new row with write_idx at the beginning
            new_row = {'write_idx': idx}
            new_row.update(row)
            writer.writerow(new_row)

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
    try:
        filter_write_statements(input_file, output_file, args.format)
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
