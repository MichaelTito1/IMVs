#!/usr/bin/env python3
"""
SQL Statement Cleaner

This script reads SQL statements from a file (one per line, ending with semicolon),
removes unsupported clauses like ORDER BY, and writes the cleaned statements
to a separate output file.
"""

import re
import argparse
import sys
from pathlib import Path


def remove_order_by(sql: str) -> str:
    """
    Removes ORDER BY clause from a SQL statement.
    Handles statements that end with semicolon.
    """
    # Check if statement ends with semicolon
    has_semicolon = sql.rstrip().endswith(';')
    
    # Remove trailing semicolon temporarily for processing
    if has_semicolon:
        sql = sql.rstrip()[:-1]
    
    # Normalize spacing
    sql = re.sub(r'\s+', ' ', sql).strip()
    
    # Remove ORDER BY clause - match until LIMIT, OFFSET, or end of string
    pattern = re.compile(
        r'\s*ORDER\s+BY\s+[^()]*?(?=\s*(?:\bLIMIT\b|\bOFFSET\b|$))',
        flags=re.IGNORECASE
    )
    sql = pattern.sub('', sql).strip()
    
    # Add semicolon back if it was there originally
    if has_semicolon:
        sql += ';'
    
    return sql


def remove_unsupported_clauses(sql: str) -> str:
    """
    Removes all unsupported clauses from a SQL statement.
    Currently removes: ORDER BY
    Can be extended to remove other clauses as needed.
    """
    # Only process SELECT statements
    if not sql.strip().upper().startswith('SELECT'):
        return sql
    
    # Remove ORDER BY clause
    sql = remove_order_by(sql)
    
    return sql


def clean_sql_file(filename: str, output_filename: str = None, dry_run: bool = False):
    """
    Clean SQL statements in a file by removing unsupported clauses.
    
    Args:
        filename: Path to the input SQL file
        output_filename: Path to the output SQL file (if None, generates from input filename)
        dry_run: If True, show what would be changed without modifying any file
    """
    file_path = Path(filename)
    
    if not file_path.exists():
        print(f"Error: File '{filename}' does not exist.")
        return False
    
    # Generate output filename if not provided
    if output_filename is None:
        output_filename = file_path.stem + '_cleaned' + file_path.suffix
        output_path = file_path.parent / output_filename
    else:
        output_path = Path(output_filename)
        # If output path is a directory, generate filename within that directory
        if output_path.is_dir():
            output_filename = file_path.stem + '_cleaned' + file_path.suffix
            output_path = output_path / output_filename
    
    try:
        # Read all lines from the file
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Process each line
        cleaned_lines = []
        changes_made = 0
        
        for line_num, line in enumerate(lines, 1):
            original_line = line.rstrip('\n\r')
            
            # Skip empty lines or comments
            if not original_line.strip() or original_line.strip().startswith('--'):
                cleaned_lines.append(line)
                continue
            
            # Clean the SQL statement
            cleaned_line = remove_unsupported_clauses(original_line)
            
            # Check if changes were made
            if cleaned_line != original_line:
                changes_made += 1
                if dry_run:
                    print(f"Line {line_num}:")
                    print(f"  BEFORE: {original_line}")
                    print(f"  AFTER:  {cleaned_line}")
                    print()
            
            # Preserve original line ending
            if line.endswith('\n'):
                cleaned_lines.append(cleaned_line + '\n')
            else:
                cleaned_lines.append(cleaned_line)
        
        if dry_run:
            print(f"DRY RUN: Would create '{output_path}' with {changes_made} statement(s) modified from '{filename}'")
            return True
        
        if changes_made == 0:
            print(f"No changes needed, but copying '{filename}' to '{output_path}'")
        
        # Write cleaned content to output file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.writelines(cleaned_lines)
        
        print(f"Successfully created '{output_path}' with {changes_made} statement(s) cleaned from '{filename}'")
        return True
        
    except Exception as e:
        print(f"Error processing file '{filename}': {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Clean SQL statements by removing unsupported clauses like ORDER BY",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
            Examples:
                python clean_read_statements.py --filename queries.sql
                python clean_read_statements.py --filename queries.sql --output cleaned_queries.sql
                python clean_read_statements.py --filename queries.sql --output /app/output_dir/
                python clean_read_statements.py --filename queries.sql --dry-run
        """
    )
    
    parser.add_argument(
        '--filename',
        type=str,
        default='/app/data/workload_200k_s1.sql',
        help='SQL file to clean (one statement per line, ending with semicolon)'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        help='Output file or directory for cleaned SQL statements (if directory provided, will use input_filename_cleaned.sql within that directory)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be changed without creating the output file'
    )
    
    args = parser.parse_args()
    
    success = clean_sql_file(
        args.filename,
        output_filename=args.output,
        dry_run=args.dry_run
    )
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()