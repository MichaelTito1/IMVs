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
        # remove _{shardNumber} suffix if present
        table_name = re.sub(r'_\d+$', '', table_name)

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
    
    read_stmts = read_sql_file(select_file)
    write_stmts = read_sql_file(write_file)

    print(f"Found {len(read_stmts)} SELECT statements and {len(write_stmts)} WRITE statements.")
    print("first 5 SELECT statements:")
    for i, stmt in enumerate(read_stmts[:5]):
        print(f"{i}: {stmt[:100]}{'...' if len(stmt) > 100 else ''}")
    print()
    print("first 5 WRITE statements:")
    for i, stmt in enumerate(write_stmts[:5]):
        print(f"{i}: {stmt[:100]}{'...' if len(stmt) > 100 else ''}")
    print()

    readStmtIdx2tables = defaultdict(list)
    for idx, stmt in enumerate(read_stmts):
        tables = extract_tables_from_select(stmt)
        readStmtIdx2tables[idx] = tables
    
    writeStmtIdx2tables = defaultdict(list)
    for idx, stmt in enumerate(write_stmts):
        tables = extract_tables_from_write(stmt)
        writeStmtIdx2tables[idx] = tables
    
    table2readStmtIdx = defaultdict(list)
    for idx, tables in readStmtIdx2tables.items():
        for table in tables:
            table2readStmtIdx[table].append(idx)

    table2WriteStmtIdx = defaultdict(list)
    for idx, tables in writeStmtIdx2tables.items():
        for table in tables:
            table2WriteStmtIdx[table].append(idx)

    matches = []
    seen_pairs = set()
    for table, read_indices in table2readStmtIdx.items():
        if table not in table2WriteStmtIdx:
            continue
        
        for read_idx in read_indices:
            for write_idx in table2WriteStmtIdx[table]:
                if (write_idx, read_idx) in seen_pairs:
                    continue
                seen_pairs.add((write_idx, read_idx))
                
                common_tables = set(readStmtIdx2tables[read_idx]) & set(writeStmtIdx2tables[write_idx])
                if common_tables:
                    matches.append({
                        'write_index': write_idx,
                        'write_statement': write_stmts[write_idx],
                        'read_index': read_idx,
                        'read_statement': read_stmts[read_idx],
                        'common_tables': sorted(common_tables)
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
    
    for match in matches:
        write_idx = match['write_index']
        read_idx = match['read_index']
        common_tables_str = ", ".join(sorted(match['common_tables']))
        
        print(f"\nWRITE {write_idx}:")
        print("-" * (len(f"WRITE {write_idx}:") + 1))
        print(f"  → SELECT {read_idx} (common tables: {common_tables_str})")
        print(f"    {match['write_statement'][:100]}{'...' if len(match['write_statement']) > 100 else ''}")
        print(f"    {match['read_statement'][:100]}{'...' if len(match['read_statement']) > 100 else ''}")


def main():
    parser = argparse.ArgumentParser(description='Match SQL write statements with relevant select statements')
    parser.add_argument('--select_file', help='File containing SELECT statements')
    parser.add_argument('--write_file', help='File containing INSERT/UPDATE/DELETE statements')
    
    args = parser.parse_args()
    
    print(f"Analyzing files:")
    print(f"  SELECT statements: {args.select_file}")
    print(f"  WRITE statements: {args.write_file}")
    print()
    
    matches = match_statements(args.select_file, args.write_file)
    
    print(f"\nSummary: Found {len(matches)} matches between write statements and select statements.")

    # save matches to a file
    if not matches:
        print("No matches found. No output file created.")
        return
    with open('matches.csv', 'w', encoding='utf-8') as f:
        f.write("write_index,write_statement,read_index,read_statement,common_tables\n")
        for match in matches:
            f.write(f"{match['write_index']},{match['write_statement']},{match['read_index']},{match['read_statement']},{';'.join(match['common_tables'])}\n")

if __name__ == "__main__":
    main()