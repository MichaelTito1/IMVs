import re
import argparse
from typing import Set, List, Dict, Tuple
from collections import defaultdict

def extract_tables_from_select(sql_statement: str) -> Set[str]:
    """Extract table names from SELECT statements."""
    # Remove comments and normalize whitespace
    sql = re.sub(r'--.*$', '', sql_statement, flags=re.MULTILINE)
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    sql = ' '.join(sql.split())
    
    tables = set()
    
    # Pattern to match FROM and JOIN clauses
    # This handles: FROM table, JOIN table, LEFT JOIN table, etc.
    from_pattern = r'\b(?:FROM|JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|INNER\s+JOIN|OUTER\s+JOIN|FULL\s+JOIN)\s+([^\s,()]+)'
    matches = re.findall(from_pattern, sql, re.IGNORECASE)
    
    for match in matches:
        # Remove schema prefix if present (schema.table -> table)
        table_name = match.split('.')[-1].strip()
        # Remove quotes if present
        table_name = table_name.strip('"\'`[]')
        if table_name and not table_name.upper() in ('ON', 'WHERE', 'GROUP', 'ORDER', 'HAVING', 'LIMIT'):
            tables.add(table_name.lower())
    
    return tables

def extract_tables_from_write(sql_statement: str) -> Set[str]:
    """Extract table names from INSERT, UPDATE, or DELETE statements."""
    # Remove comments and normalize whitespace
    sql = re.sub(r'--.*$', '', sql_statement, flags=re.MULTILINE)
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    sql = ' '.join(sql.split())
    
    tables = set()
    
    # Pattern for INSERT INTO
    insert_pattern = r'\bINSERT\s+INTO\s+([^\s(]+)'
    insert_matches = re.findall(insert_pattern, sql, re.IGNORECASE)
    
    # Pattern for UPDATE
    update_pattern = r'\bUPDATE\s+([^\s]+)'
    update_matches = re.findall(update_pattern, sql, re.IGNORECASE)
    
    # Pattern for DELETE FROM
    delete_pattern = r'\bDELETE\s+FROM\s+([^\s]+)'
    delete_matches = re.findall(delete_pattern, sql, re.IGNORECASE)
    
    all_matches = insert_matches + update_matches + delete_matches
    
    for match in all_matches:
        # Remove schema prefix if present
        table_name = match.split('.')[-1].strip()
        # Remove quotes if present
        table_name = table_name.strip('"\'`[]')
        if table_name:
            tables.add(table_name.lower())
    
    # Also check for tables in JOIN clauses within UPDATE/DELETE statements
    join_tables = extract_tables_from_select(sql)
    tables.update(join_tables)
    
    return tables

def read_sql_file(filename: str) -> List[str]:
    """Read SQL file and return list of non-empty statements."""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Filter out empty lines and strip whitespace
        statements = [line.strip() for line in lines if line.strip()]
        return statements
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
        return []
    except Exception as e:
        print(f"Error reading file '{filename}': {e}")
        return []

def match_statements(select_file: str, write_file: str) -> Dict[str, List[str]]:
    """Match write statements with relevant select statements."""
    
    # Read files
    select_statements = read_sql_file(select_file)
    write_statements = read_sql_file(write_file)
    
    if not select_statements or not write_statements:
        return {}
    
    # Extract tables for each statement
    select_tables = []
    write_tables = []
    
    print("Processing SELECT statements...")
    for i, stmt in enumerate(select_statements):
        tables = extract_tables_from_select(stmt)
        select_tables.append(tables)
        print(f"  SELECT {i+1}: {tables}")
    
    print("\nProcessing WRITE statements...")
    for i, stmt in enumerate(write_statements):
        tables = extract_tables_from_write(stmt)
        write_tables.append(tables)
        stmt_type = "INSERT" if "INSERT" in stmt.upper() else "UPDATE" if "UPDATE" in stmt.upper() else "DELETE"
        print(f"  {stmt_type} {i+1}: {tables}")
    
    # Match statements
    matches = defaultdict(list)
    
    print("\nMatching statements...")
    for write_idx, write_tbls in enumerate(write_tables):
        write_stmt = write_statements[write_idx]
        stmt_type = "INSERT" if "INSERT" in write_stmt.upper() else "UPDATE" if "UPDATE" in write_stmt.upper() else "DELETE"
        
        for select_idx, select_tbls in enumerate(select_tables):
            # Check if there's any common table
            common_tables = write_tbls.intersection(select_tbls)
            if common_tables:
                match_key = f"{stmt_type} {write_idx + 1}"
                matches[match_key].append({
                    'select_index': select_idx + 1,
                    'select_statement': select_statements[select_idx],
                    'common_tables': common_tables
                })
    
    return matches

def print_matches(matches: Dict[str, List[str]]):
    """Print the matching results in a readable format."""
    if not matches:
        print("No matches found between write and select statements.")
        return
    
    print("\n" + "="*80)
    print("MATCHING RESULTS")
    print("="*80)
    
    for write_key, select_matches in matches.items():
        print(f"\n{write_key}:")
        print("-" * (len(write_key) + 1))
        
        for match in select_matches:
            common_tables_str = ", ".join(sorted(match['common_tables']))
            print(f"  → SELECT {match['select_index']} (common tables: {common_tables_str})")
            print(f"    {match['select_statement'][:100]}{'...' if len(match['select_statement']) > 100 else ''}")

def main():
    parser = argparse.ArgumentParser(description='Match SQL write statements with relevant select statements')
    parser.add_argument('--select_file', help='File containing SELECT statements')
    parser.add_argument('--write_file', help='File containing INSERT/UPDATE/DELETE statements')
    parser.add_argument('--verbose', '-v', action='store_true', help='Show detailed output')
    
    args = parser.parse_args()
    
    print(f"Analyzing files:")
    print(f"  SELECT statements: {args.select_file}")
    print(f"  WRITE statements: {args.write_file}")
    print()
    
    matches = match_statements(args.select_file, args.write_file)
    print_matches(matches)
    
    # Summary
    total_matches = sum(len(select_list) for select_list in matches.values())
    print(f"\nSummary: Found {total_matches} total matches between {len(matches)} write statements and select statements.")

if __name__ == "__main__":
    main()