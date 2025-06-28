import re
import argparse
import csv
import gc
import time
from typing import Set, List, Dict, Iterator, Tuple
from collections import defaultdict, Counter

def extract_tables_from_select(sql_statement: str) -> Set[str]:
    """Extract table names from SELECT statements."""
    # Remove comments and normalize whitespace
    sql = re.sub(r'--.*$', '', sql_statement, flags=re.MULTILINE)
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    sql = ' '.join(sql.split())
    
    tables = set()
    
    # Pattern to match FROM and JOIN clauses
    from_pattern = r'\b(?:FROM|JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|INNER\s+JOIN|OUTER\s+JOIN|FULL\s+JOIN|CROSS\s+JOIN)\s+([^\s,()]+)'
    matches = re.findall(from_pattern, sql, re.IGNORECASE)
    
    for match in matches:
        # Remove schema prefix if present (schema.table -> table)
        table_name = match.split('.')[-1].strip()
        # Remove quotes if present
        table_name = table_name.strip('"\'`[]')
        # Remove _{shardNumber} suffix if present
        table_name = re.sub(r'_\d+$', '', table_name)
        
        if table_name and not table_name.upper() in ('ON', 'WHERE', 'GROUP', 'ORDER', 'HAVING', 'LIMIT', 'UNION', 'SELECT'):
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
        # Remove _{shardNumber} suffix if present
        table_name = re.sub(r'_\d+$', '', table_name)

        if table_name:
            tables.add(table_name.lower())
    
    # Also check for tables in JOIN clauses within UPDATE/DELETE statements
    join_tables = extract_tables_from_select(sql)
    tables.update(join_tables)
    
    return tables

def count_statements(filename: str) -> int:
    """Count total statements in file."""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            content = f.read()
        
        if ';' in content:
            statements = [stmt.strip() for stmt in content.split(';') if stmt.strip()]
        else:
            statements = [line.strip() for line in content.split('\n') if line.strip()]
            
        return len(statements)
    except Exception as e:
        print(f"Error counting statements in {filename}: {e}")
        return 0

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

class WriteStatementIndex:
    """Class to manage write statement indexing with proper data integrity."""
    
    def __init__(self, max_writes_per_table: int = 100):
        self.max_writes_per_table = max_writes_per_table
        self.table_to_writes = defaultdict(list)  # table -> list of write_ids
        self.write_data = {}  # write_id -> {'statement': str, 'tables': set}
        self.table_write_counts = defaultdict(int)
    
    def add_write_statement(self, write_id: int, statement: str, tables: Set[str]) -> bool:
        """Add a write statement to the index. Returns True if added, False if skipped."""
        if not tables:
            return False
        
        # Store the write statement data
        self.write_data[write_id] = {
            'statement': statement,
            'tables': tables
        }
        
        # Add to table index only if under limit
        added_to_any_table = False
        for table in tables:
            if self.table_write_counts[table] < self.max_writes_per_table:
                self.table_to_writes[table].append(write_id)
                self.table_write_counts[table] += 1
                added_to_any_table = True
        
        return added_to_any_table
    
    def get_writes_for_table(self, table: str) -> List[int]:
        """Get write statement IDs for a given table."""
        return self.table_to_writes.get(table, [])
    
    def get_write_data(self, write_id: int) -> Dict:
        """Get write statement data by ID."""
        return self.write_data.get(write_id, {'statement': '', 'tables': set()})
    
    def get_stats(self) -> Dict:
        """Get statistics about the index."""
        return {
            'total_writes': len(self.write_data),
            'indexed_writes': sum(len(writes) for writes in self.table_to_writes.values()),
            'unique_tables': len(self.table_to_writes),
            'table_counts': dict(self.table_write_counts)
        }

def build_write_index_smart(write_file: str, max_writes_per_table: int = 100) -> WriteStatementIndex:
    """Build a SMART index that limits writes per table to prevent explosion."""
    print("Building smart write statement index...")
    
    index = WriteStatementIndex(max_writes_per_table)
    total_writes = count_statements(write_file)
    print(f"Total WRITE statements: {total_writes}")
    
    count = 0
    added_count = 0
    
    for write_idx, write_stmt in read_sql_statements(write_file):
        tables = extract_tables_from_write(write_stmt)
        if index.add_write_statement(write_idx, write_stmt, tables):
            added_count += 1
        
        count += 1
        if count % 2000 == 0:
            print(f"  Processed {count}/{total_writes} WRITE statements, indexed {added_count}...")
    
    stats = index.get_stats()
    print(f"Indexed {stats['total_writes']} WRITE statements")
    print(f"Table distribution (limited to {max_writes_per_table} writes per table):")
    for table, count in sorted(stats['table_counts'].items()):
        print(f"  {table}: {count} writes")
    
    return index

def process_with_limits(select_file: str, write_file: str, 
                       max_writes_per_table: int = 100,
                       max_matches_per_select: int = 50,
                       max_total_matches: int = 100000) -> None:
    """Process with strict limits to prevent explosion."""
    
    total_selects = count_statements(select_file)
    print(f"Total SELECT statements: {total_selects}")
    
    # Build the write statement index with limits
    write_index = build_write_index_smart(write_file, max_writes_per_table)
    
    print(f"\nProcessing SELECT statements with limits:")
    print(f"  Max writes per table: {max_writes_per_table}")
    print(f"  Max matches per SELECT: {max_matches_per_select}")
    print(f"  Max total matches: {max_total_matches}")
    
    # Open output file with explicit encoding and proper CSV settings
    output_file = 'matches.csv'
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['select_id', 'select_tables', 
                     'write_id', 'write_tables', 'common_tables']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        
        total_matches = 0
        processed_selects = 0
        start_time = time.time()
        
        # Process SELECT statements
        for select_idx, select_stmt in read_sql_statements(select_file):
            if total_matches >= max_total_matches:
                print(f"Reached maximum total matches ({max_total_matches}). Stopping.")
                break
                
            select_tables = extract_tables_from_select(select_stmt)
            
            if select_tables:
                matches_for_this_select = 0
                batch_matches = []
                processed_pairs = set()
                
                # Find matching write statements with limits
                for table in select_tables:
                    if matches_for_this_select >= max_matches_per_select:
                        break
                    
                    write_ids = write_index.get_writes_for_table(table)
                    for write_id in write_ids:
                        if matches_for_this_select >= max_matches_per_select:
                            break
                            
                        pair_key = (select_idx, write_id)
                        if pair_key not in processed_pairs:
                            write_data = write_index.get_write_data(write_id)
                            write_stmt = write_data['statement']
                            write_tables = write_data['tables']
                            
                            # Debug: Validate write data integrity
                            if not write_stmt or not write_tables:
                                print(f"Warning: Invalid write data for write_id {write_id}")
                                print(f"  Statement: {repr(write_stmt[:100])}")
                                print(f"  Tables: {write_tables}")
                                continue
                            
                            common_tables = select_tables & write_tables
                            
                            if common_tables:
                                # Ensure all values are properly formatted and not None
                                truncated_select = select_stmt[:500] + '...' if len(select_stmt) > 500 else select_stmt
                                truncated_write = write_stmt[:500] + '...' if len(write_stmt) > 500 else write_stmt
                                
                                match = {
                                    'select_id': str(select_idx),
                                    # 'select_statement': truncated_select.replace('\n', ' ').replace('\r', ''),
                                    'select_tables': ';'.join(sorted(select_tables)),
                                    'write_id': str(write_id),
                                    # 'write_statement': truncated_write.replace('\n', ' ').replace('\r', ''),
                                    'write_tables': ';'.join(sorted(write_tables)),
                                    'common_tables': ';'.join(sorted(common_tables)),
                                }
                                
                                batch_matches.append(match)
                                processed_pairs.add(pair_key)
                                matches_for_this_select += 1
                
                # Write matches for this SELECT one by one to ensure data integrity
                if batch_matches:
                    for match in batch_matches:
                        # Validate that all required fields are present and properly formatted
                        if all(key in match and match[key] is not None for key in fieldnames):
                            writer.writerow(match)
                            total_matches += 1
                        else:
                            print(f"Warning: Skipping invalid match for select_id {select_idx}")
                            print(f"Match data: {match}")
            
            processed_selects += 1
            
            # Progress update
            if processed_selects % 1000 == 0:
                elapsed = time.time() - start_time
                rate = processed_selects / elapsed
                eta = (total_selects - processed_selects) / rate if rate > 0 else 0
                print(f"  Processed {processed_selects}/{total_selects} SELECT statements, "
                      f"found {total_matches} matches, "
                      f"ETA: {eta:.1f}s")
                
                # Periodic garbage collection
                gc.collect()
    
    elapsed = time.time() - start_time
    print(f"\n=== FINAL SUMMARY ===")
    print(f"Total SELECT statements processed: {processed_selects}")
    print(f"Total matches found: {total_matches}")
    print(f"Processing time: {elapsed:.1f} seconds")
    print(f"Results saved to: {output_file}")
    
    if total_matches >= max_total_matches:
        print(f"\nWARNING: Hit maximum match limit ({max_total_matches}). "
              f"Increase --max-total-matches if you need more results.")

def quick_analysis(select_file: str, write_file: str) -> None:
    """Quick analysis of the files."""
    print("=== QUICK ANALYSIS ===")
    
    # Count total statements
    total_selects = count_statements(select_file)
    total_writes = count_statements(write_file)
    print(f"Total SELECT statements: {total_selects}")
    print(f"Total WRITE statements: {total_writes}")
    
    # Sample first few statements
    print("\nFirst 3 SELECT statements:")
    for i, (idx, stmt) in enumerate(read_sql_statements(select_file)):
        if i >= 3:
            break
        print(f"  {idx}: {stmt[:100]}{'...' if len(stmt) > 100 else ''}")
    
    print("\nFirst 3 WRITE statements:")
    for i, (idx, stmt) in enumerate(read_sql_statements(write_file)):
        if i >= 3:
            break
        print(f"  {idx}: {stmt[:100]}{'...' if len(stmt) > 100 else ''}")
    
    # Analyze table distribution
    select_table_counts = Counter()
    write_table_counts = Counter()
    
    sample_size = 1000
    print(f"\nAnalyzing table distribution (sample of {sample_size} statements each)...")
    
    for i, (idx, stmt) in enumerate(read_sql_statements(select_file)):
        if i >= sample_size:
            break
        tables = extract_tables_from_select(stmt)
        for table in tables:
            select_table_counts[table] += 1
    
    for i, (idx, stmt) in enumerate(read_sql_statements(write_file)):
        if i >= sample_size:
            break
        tables = extract_tables_from_write(stmt)
        for table in tables:
            write_table_counts[table] += 1
    
    common_tables = set(select_table_counts.keys()) & set(write_table_counts.keys())
    
    print(f"SELECT table frequency (top 10):")
    for table, count in select_table_counts.most_common(10):
        print(f"  {table}: {count}")
    
    print(f"WRITE table frequency (top 10):")
    for table, count in write_table_counts.most_common(10):
        print(f"  {table}: {count}")
    
    print(f"Common tables: {len(common_tables)}")
    print(f"Common tables: {sorted(common_tables)}")
    
    # Estimate potential matches
    if common_tables:
        avg_select_per_table = sum(select_table_counts[t] for t in common_tables) / len(common_tables)
        avg_write_per_table = sum(write_table_counts[t] for t in common_tables) / len(common_tables)
        estimated_matches = avg_select_per_table * avg_write_per_table * len(common_tables)
        print(f"Estimated matches (rough): {estimated_matches:.0f}")
        
        if estimated_matches > 1000000:
            print("WARNING: Very high number of potential matches. Use strict limits!")

def main():
    parser = argparse.ArgumentParser(description='Match SQL statements with smart limits')
    parser.add_argument('--select_file', required=True, help='File containing SELECT statements')
    parser.add_argument('--write_file', required=True, help='File containing INSERT/UPDATE/DELETE statements')
    parser.add_argument('--max-writes-per-table', type=int, default=20, 
                       help='Max write statements per table (default: 20)')
    parser.add_argument('--max-matches-per-select', type=int, default=20, 
                       help='Max matches per SELECT statement (default: 20)')
    parser.add_argument('--max-total-matches', type=int, default=50000, 
                       help='Max total matches to find (default: 50000)')
    parser.add_argument('--analysis-only', action='store_true', 
                       help='Only run analysis, do not process matches')
    
    args = parser.parse_args()
    
    print(f"SQL Statement Matcher (Smart Limits)")
    print(f"====================================")
    print(f"SELECT file: {args.select_file}")
    print(f"WRITE file:  {args.write_file}")
    print()
    
    quick_analysis(args.select_file, args.write_file)
    
    if not args.analysis_only:
        print(f"\nStarting processing with limits:")
        print(f"  Max writes per table: {args.max_writes_per_table}")
        print(f"  Max matches per SELECT: {args.max_matches_per_select}")
        print(f"  Max total matches: {args.max_total_matches}")
        
        process_with_limits(args.select_file, args.write_file,
                          args.max_writes_per_table,
                          args.max_matches_per_select,
                          args.max_total_matches)

if __name__ == "__main__":
    main()