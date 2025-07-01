#!/usr/bin/env python3
"""
SQL Statement Cleaner

This script reads SQL statements from a file (one per line, ending with semicolon),
removes unsupported clauses like ORDER BY, and rewrites the cleaned statements
back to the file in place.
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


def clean_sql_file(filename: str, backup: bool = True, dry_run: bool = False):
    """
    Clean SQL statements in a file by removing unsupported clauses.
    
    Args:
        filename: Path to the SQL file
        backup: Whether to create a backup file before modifying
        dry_run: If True, show what would be changed without modifying the file
    """
    file_path = Path(filename)
    
    if not file_path.exists():
        print(f"Error: File '{filename}' does not exist.")
        return False
    
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
            print(f"DRY RUN: Would modify {changes_made} statement(s) in '{filename}'")
            return True
        
        if changes_made == 0:
            print(f"No changes needed in '{filename}'")
            return True
        
        # Create backup if requested
        if backup:
            backup_path = file_path.with_suffix(file_path.suffix + '.backup')
            with open(backup_path, 'w', encoding='utf-8') as f:
                f.writelines(lines)
            print(f"Backup created: {backup_path}")
        
        # Write cleaned content back to file
        with open(file_path, 'w', encoding='utf-8') as f:
            f.writelines(cleaned_lines)
        
        print(f"Successfully cleaned {changes_made} statement(s) in '{filename}'")
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
  python sql_cleaner.py queries.sql
  python sql_cleaner.py queries.sql --no-backup
  python sql_cleaner.py queries.sql --dry-run
        """
    )
    
    parser.add_argument(
        '--filename',
        type=str,
        default='/app/data/imv_test_workload.sql',
        help='SQL file to clean (one statement per line, ending with semicolon)'
    )
    
    parser.add_argument(
        '--no-backup',
        action='store_true',
        help='Do not create a backup file before modifying'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be changed without modifying the file'
    )
    
    args = parser.parse_args()
    
    success = clean_sql_file(
        args.filename,
        backup=not args.no_backup,
        dry_run=args.dry_run
    )
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()