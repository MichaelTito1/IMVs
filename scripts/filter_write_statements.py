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

def quote_table_names(sql_statement):
    """
    Encapsulate all table names in SQL statements with double quotes.
    Handles INSERT, UPDATE, DELETE statements and their various clauses.
    """
    if not sql_statement or not sql_statement.strip():
        return sql_statement
    
    sql = sql_statement.strip()
    
    def quote_identifier(identifier):
        """Helper function to quote an identifier if it's not already quoted."""
        identifier = identifier.strip()
        # Remove existing quotes of any type to normalize
        if identifier.startswith('"') and identifier.endswith('"'):
            identifier = identifier[1:-1]
        elif identifier.startswith("'") and identifier.endswith("'"):
            identifier = identifier[1:-1]
        elif identifier.startswith("`") and identifier.endswith("`"):
            identifier = identifier[1:-1]
        elif identifier.startswith("[") and identifier.endswith("]"):
            identifier = identifier[1:-1]
        
        # Return with double quotes
        return f'"{identifier}"'
    
    # Pattern for INSERT INTO table_name
    def replace_insert(match):
        table_name = match.group(1)
        return f'INSERT INTO {quote_identifier(table_name)}'
    sql = re.sub(r'\bINSERT\s+INTO\s+([^\s(,;]+)', replace_insert, sql, flags=re.IGNORECASE)
    
    # Pattern for UPDATE table_name
    def replace_update(match):
        table_name = match.group(1)
        return f'UPDATE {quote_identifier(table_name)}'
    sql = re.sub(r'\bUPDATE\s+([^\s,;]+)', replace_update, sql, flags=re.IGNORECASE)
    
    # Pattern for DELETE FROM table_name
    def replace_delete(match):
        table_name = match.group(1)
        return f'DELETE FROM {quote_identifier(table_name)}'
    sql = re.sub(r'\bDELETE\s+FROM\s+([^\s,;]+)', replace_delete, sql, flags=re.IGNORECASE)
    
    # Pattern for USING table_name (in DELETE statements)
    def replace_using(match):
        table_name = match.group(1)
        return f'USING {quote_identifier(table_name)}'
    sql = re.sub(r'\bUSING\s+([^\s,;]+)', replace_using, sql, flags=re.IGNORECASE)
    
    # Pattern for FROM table_name
    def replace_from(match):
        table_name = match.group(1)
        return f'FROM {quote_identifier(table_name)}'
    sql = re.sub(r'\bFROM\s+([^\s,;()]+)', replace_from, sql, flags=re.IGNORECASE)
    
    # Pattern for JOIN table_name (including LEFT JOIN, RIGHT JOIN, INNER JOIN, etc.)
    def replace_join(match):
        join_type = match.group(0).rsplit(' ', 1)[0]  # Everything except the table name
        table_name = match.group(1)
        return f'{join_type} {quote_identifier(table_name)}'
    sql = re.sub(r'\b(?:LEFT\s+|RIGHT\s+|INNER\s+|OUTER\s+|FULL\s+|CROSS\s+)?JOIN\s+([^\s,;()]+)', 
                 replace_join, sql, flags=re.IGNORECASE)
    
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
            # stmt['sql'] = add_suffix_to_sql(cleaned_sql)
            quoted_sql = quote_table_names(cleaned_sql)
            stmt['sql'] = quoted_sql
    
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
    
    example_sql = """
    DELETE FROM "rel13" USING "inf" WHERE "rel13"."a_id" = "inf"."a_id" AND "inf"."dur" BETWEEN '3' AND '3';
    """
    print(f"Example SQL after quoting table names:\n{quote_table_names(example_sql)}\n")

    example_sql = """
    INSERT INTO "rel13" ("a_id", "m_id") SELECT DISTINCT ON("rel13"."a_id", "rel13"."m_id") "rel13"."a_id", "rel13"."m_id" FROM "rel13" ON CONFLICT ("a_id", "m_id") DO NOTHING;
    """
    print(f"Example SQL after quoting table names:\n{quote_table_names(example_sql)}\n")

    example_sql = """
    SELECT COUNT(*) as agg_0 FROM "rel13" JOIN "dispat" ON "rel13"."m_id" = "dispat"."m_id" JOIN "rel12" ON "dispat"."m_id" = "rel12"."m_id" WHERE "rel13"."m_id" <= 991739 AND "rel12"."m_id" <= 627690 AND "dispat"."m_id" >= 892446;
    """
    print(f"Example SQL after quoting table names:\n{quote_table_names(example_sql)}\n")
    try:
        filter_write_statements(input_file, output_file, args.format)
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
