# postgresql_benchmark.py
import csv
import time
import logging
from typing import Any, Dict, List, Tuple
import json
import re

from classes.baseball_db import BaseballDB
from utils import remove_table_suffixes

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PostgreSQLBenchmark(BaseballDB):
    def __init__(self, host='localhost', port=5432, user='postgres', password='', db_name='baseball'):
        # Initialize parent class
        super().__init__(host=host, port=port, user=user, password=password, db_name=db_name)
        self.results = []
        self._metadata_cache = {}  # Cache for write statement metadata
        self._metadata_loaded = False
        self._last_saved_index = 0  # Track how many results we've already saved
        
    def connect(self):
        """Connect to PostgreSQL database and enable pg_ivm extension"""
        # Use parent's connection method
        if not self.connect_to_target_db():
            return False
            
        # Enable pg_ivm extension
        try:
            self.cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_ivm;")
            self.conn.commit()
            logger.info("pg_ivm extension enabled")
        except Exception as e:
            logger.warning(f"Could not enable pg_ivm extension: {e}", exc_info=True)
        
        # Enable mv_stats extension
        try:
            self.cursor.execute("CREATE EXTENSION IF NOT EXISTS mv_stats;")
            self.conn.commit()
            logger.info("mv_stats extension enabled")
        except Exception as e:
            logger.warning(f"Could not enable mv_stats extension: {e}", exc_info=True)
        return True
    
    def warmup_database(self, select_statements: List[str], warmup_rounds: int = 3):
        """Warmup the database with simple SELECT queries"""
        logger.info(f"Starting database warmup with {warmup_rounds} rounds...")
        
        for round_num in range(warmup_rounds):
            logger.info(f"Warmup round {round_num + 1}/{warmup_rounds}")
            for idx, statement in enumerate(select_statements[:10]):  # Limit to first 10 for warmup
                try:
                    start_time = time.time()
                    self.cursor.execute(statement)
                    self.cursor.fetchall()  # Ensure all results are fetched
                    execution_time = (time.time() - start_time) * 1000
                    
                    if round_num == 0:  # Only log first round to avoid spam
                        logger.debug(f"Warmup query {idx}: {execution_time:.4f}s")
                        
                except Exception as e:
                    logger.warning(f"Warmup query {idx} failed: {e}", exc_info=True)
        
        logger.info("Database warmup completed")
    
    def execute_with_timing_and_plan(self, statement: str, statement_type: str = "SELECT", fetch_results: bool = True) -> Dict[str, Any]:
        """
        Execute a statement and collect timing and execution plan.
        """
        result = {
            'statement': statement,
            'statement_type': statement_type,
            'execution_time': 0,
            'plan': None,
            'rows_affected': 0,
            'rows_inserted': 0,
            'error': None
        }
        
        try:
            # Get execution plan for SELECT statements
            if statement_type == "SELECT":
                plan_query = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {statement}"
                start_time = time.time()
                self.cursor.execute(plan_query)
                plan_result = self.cursor.fetchone()[0]
                execution_time = (time.time() - start_time) * 1000
                
                result['execution_time'] = execution_time
                result['plan_execution_time'] = plan_result[0]['Execution Time'] if plan_result else None
                result['plan'] = plan_result[0]['Plan'] if plan_result else None
                
                # Get actual row count
                if result['plan'] and isinstance(result['plan'], dict):
                    try:
                        result['rows_affected'] = result['plan']['Actual Rows']
                    except (KeyError, TypeError) as e:
                        logger.info(f"Could not extract Actual Rows from plan. Error: {e}", exc_info=True)
                        pass
            else:
                # For write operations, execute with EXPLAIN ANALYZE and extract rows inserted
                plan_query = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {statement}"
                start_time = time.time()
                self.cursor.execute(plan_query)
                plan_result = self.cursor.fetchone()[0]
                execution_time = (time.time() - start_time) * 1000
                
                result['execution_time'] = execution_time
                result['plan_execution_time'] = plan_result[0]['Execution Time'] if plan_result else None
                result['plan'] = plan_result[0]['Plan'] if plan_result else None
                result['triggers'] = plan_result[0]['Triggers'] if 'Triggers' in plan_result[0] else None
                result['rows_affected'] = result['plan']['Actual Rows']
                
                # Extract rows inserted from plan for INSERT operations
                if statement.strip().upper().startswith('INSERT') and result['plan']:
                    try:
                        result['rows_inserted'] = self._extract_rows_inserted_from_plan(result['plan'])
                    except Exception as e:
                        logger.debug(f"Could not extract rows_inserted from plan: {e}")
                
        except Exception as e:
            result['error'] = str(e)
            logger.error(f"Error executing statement: {e}", exc_info=True)
            
        return result

    def _extract_rows_inserted_from_plan(self, plan: Dict) -> int:
        """
        Extract the number of rows inserted from the execution plan.
        Looks for Insert node in the plan tree.
        """
        if not plan:
            return 0
        
        def find_insert_rows(node):
            if isinstance(node, dict):
                if node.get('Node Type') == 'Insert':
                    return node.get('Actual Rows', 0)
                
                # Check plans array
                if 'Plans' in node:
                    for subplan in node['Plans']:
                        rows = find_insert_rows(subplan)
                        if rows > 0:
                            return rows
            
            return 0
        
        return find_insert_rows(plan)
    
    def create_materialized_view(self, select_statement: str, view_name: str) -> bool:
        """Create a materialized view from a SELECT statement"""
        try:
            # Clean the view name
            clean_view_name = f"mv_{view_name}".replace(' ', '_').replace('-', '_')
            
            mv_sql = f"CREATE MATERIALIZED VIEW {clean_view_name} AS {select_statement}"
            self.cursor.execute(mv_sql)
            self.conn.commit()
            logger.info(f"Created materialized view: {clean_view_name}")
            return True
        except Exception as e:
            logger.error(f"Error creating materialized view {clean_view_name}: {e}", exc_info=True)
            self.conn.rollback()
            return False
    
    def create_incremental_view(self, select_statement: str, view_name: str) -> bool:
        """Create an incremental materialized view using pg_ivm"""
        try:
            # Clean the view name
            clean_view_name = f"imv_{view_name}".replace(' ', '_').replace('-', '_')
            select_statement = select_statement.replace(';', '').replace('\'', '\'\'')
            imv_sql = f"SELECT pgivm.create_immv('{clean_view_name}', '{select_statement}')"
            self.cursor.execute(imv_sql)
            self.conn.commit()
            logger.info(f"Created incremental materialized view: {clean_view_name}")
            return True
        except Exception as e:
            logger.error(f"Error creating incremental materialized view {clean_view_name}: {e}", exc_info=True)
            logger.error(f"SQL: {imv_sql}")
            self.conn.rollback()
            return False
    
    def refresh_materialized_view(self, view_name: str) -> Dict[str, Any]:
        """Refresh a materialized view and return timing info"""
        result = {
            'operation': 'refresh_mv',
            'view_name': view_name,
            'execution_time': 0,
            'error': None
        }
        
        try:
            self.cursor.execute(f"REFRESH MATERIALIZED VIEW {view_name}")
            self.conn.commit()
            self.cursor.execute(f"SELECT refresh_mv_time_last FROM mv_stats WHERE mv_name = 'public.{view_name}'")
            fetch_result = self.cursor.fetchone()
            time_delta = fetch_result[0] if fetch_result else None
            result['execution_time'] = time_delta.total_seconds() * 1000
        except Exception as e:
            result['error'] = str(e)
            logger.error(f"Error refreshing materialized view {view_name}: {e}", exc_info=True)
            
        return result
    
    def cleanup_views(self):
        """Drop all created materialized and incremental views, and tables matching 'imv_exp_%'."""
        try:
            # Drop materialized views
            self.cursor.execute("""
                SELECT schemaname, matviewname
                FROM pg_matviews
                WHERE matviewname LIKE 'mv_%' OR matviewname LIKE 'imv_%'
            """)
            views = self.cursor.fetchall()

            for schema, view_name in views:
                try:
                    self.cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {schema}.{view_name} CASCADE")
                    logger.debug(f"Dropped materialized view: {schema}.{view_name}")
                except Exception as e:
                    logger.warning(f"Error dropping materialized view {schema}.{view_name}: {e}", exc_info=True)

            # Drop tables matching 'imv_exp_%'
            self.cursor.execute("""
                SELECT schemaname, tablename
                FROM pg_tables
                WHERE tablename LIKE 'imv_exp_%'
            """)
            tables = self.cursor.fetchall()

            for schema, table_name in tables:
                try:
                    self.cursor.execute(f"DROP TABLE IF EXISTS {schema}.{table_name} CASCADE")
                    logger.debug(f"Dropped table: {schema}.{table_name}")
                except Exception as e:
                    logger.warning(f"Error dropping table {schema}.{table_name}: {e}", exc_info=True)

            self.conn.commit()
            logger.info("Cleanup of views and tables completed")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error during cleanup: {e}", exc_info=True)

    
    def run_experiment(self, select_statement: str, write_statements: List[Tuple[int, str]], experiment_id: str, use_batch: bool = False) -> List[Dict[str, Any]]:
        """
        Run a complete experiment with all three configurations.
        """
        experiment_results = []
        
        logger.info(f"Starting experiment {experiment_id} (batch_mode: {use_batch})")
        
        # Prepare write statements based on batch mode
        if use_batch and len(write_statements) > 1:
            batched_write = self.batch_write_statements(write_statements)
            write_ops = [batched_write] if batched_write else write_statements
            batch_size = len(write_statements)
        else:
            write_ops = write_statements
            batch_size = 1
        
        # Configuration 1: Basic setup (no views)
        logger.info(f"Experiment {experiment_id} - Basic setup")
        try:
            # Execute SELECT statement
            select_result = self.execute_with_timing_and_plan(select_statement, "SELECT")
            select_result.update({
                'experiment_id': experiment_id,
                'configuration': 'basic',
                'operation_type': 'select',
                'batch_mode': use_batch,
                'batch_size': batch_size
            })
            experiment_results.append(select_result)
            
            # Execute WRITE statements
            idx = 0
            for write_id, write_statement in write_ops:
                logger.info(f"Executing write statement {idx + 1}/{len(write_ops)}")
                idx += 1

                write_result = self.execute_with_timing_and_plan(write_statement, "WRITE", fetch_results=False)
                write_result.update({
                    'experiment_id': experiment_id,
                    'configuration': 'basic',
                    'operation_type': 'write',
                    'write_index': write_id,
                    'batch_mode': use_batch,
                    'batch_size': batch_size
                })
                experiment_results.append(write_result)
            
            self.conn.commit()
            
        except Exception as e:
            logger.error(f"Error in basic setup for experiment {experiment_id}: {e}", exc_info=True)
            self.conn.rollback()
        
        # Configuration 2: Materialized View setup
        logger.info(f"Experiment {experiment_id} - Materialized View setup")
        try:
            view_name = f"mv_{experiment_id}"
            
            if self.create_materialized_view(select_statement, experiment_id):
                # Execute SELECT on materialized view
                mv_select = f"SELECT * FROM {view_name}"
                select_result = self.execute_with_timing_and_plan(mv_select, "SELECT")
                select_result.update({
                    'experiment_id': experiment_id,
                    'configuration': 'materialized_view',
                    'operation_type': 'select',
                    'batch_mode': use_batch,
                    'batch_size': batch_size
                })
                experiment_results.append(select_result)
                
                # Execute WRITE statements with MV refresh
                for write_id, write_statement in write_ops:
                    # Execute write
                    write_result = self.execute_with_timing_and_plan(write_statement, "WRITE", fetch_results=False)
                    write_result.update({
                        'experiment_id': experiment_id,
                        'configuration': 'materialized_view',
                        'operation_type': 'write',
                        'write_index': write_id,
                        'batch_mode': use_batch,
                        'batch_size': batch_size
                    })
                    experiment_results.append(write_result)
                    
                    # Refresh materialized view
                    refresh_result = self.refresh_materialized_view(view_name)
                    refresh_result.update({
                        'experiment_id': experiment_id,
                        'configuration': 'materialized_view',
                        'operation_type': 'refresh',
                        'write_index': write_id,
                        'batch_mode': use_batch,
                        'batch_size': batch_size
                    })
                    experiment_results.append(refresh_result)
                
                # Cleanup MV
                self.cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name}")
                self.conn.commit()
            
        except Exception as e:
            logger.error(f"Error in materialized view setup for experiment {experiment_id}: {e}", exc_info=True)
            self.conn.rollback()
        
        # Configuration 3: Incremental Materialized View setup
        logger.info(f"Experiment {experiment_id} - Incremental Materialized View setup")
        try:
            imv_name = f"imv_{experiment_id}"
            
            if self.create_incremental_view(select_statement, experiment_id):
                # Execute SELECT on incremental view
                imv_select = f"SELECT * FROM {imv_name}"
                select_result = self.execute_with_timing_and_plan(imv_select, "SELECT")
                select_result.update({
                    'experiment_id': experiment_id,
                    'configuration': 'incremental_view',
                    'operation_type': 'select',
                    'batch_mode': use_batch,
                    'batch_size': batch_size
                })
                experiment_results.append(select_result)
                
                # Execute WRITE statements (IMV updates automatically)
                for write_id, write_statement in write_ops:
                    write_result = self.execute_with_timing_and_plan(write_statement, "WRITE", fetch_results=False)
                    write_result.update({
                        'experiment_id': experiment_id,
                        'configuration': 'incremental_view',
                        'operation_type': 'write',
                        'write_index': write_id,
                        'batch_mode': use_batch,
                        'batch_size': batch_size
                    })
                    experiment_results.append(write_result)
            
            self.conn.commit()
            
        except Exception as e:
            logger.error(f"Error in incremental view setup for experiment {experiment_id}: {e}", exc_info=True)
            self.conn.rollback()
        
        logger.info(f"Completed experiment {experiment_id}")
        return experiment_results

    def _load_write_metadata_cache(self):
        """Load all write statement metadata into cache once"""
        if self._metadata_loaded:
            return
            
        try:
            # TODO: Specify argument for path to write_workload.csv
            with open('/app/data/write_workload.csv', 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for idx, row in enumerate(reader):
                    self._metadata_cache[idx] = {
                        'query_type': row.get('query_type', ''),
                        'num_joins': int(row.get('num_joins', 0)),
                        'num_scans': int(row.get('num_scans', 0)),
                        'num_aggregations': int(row.get('num_aggregations', 0)),
                        'start_table': remove_table_suffixes(row.get('start_t', '')),
                        'join_tables': [remove_table_suffixes(t) for t in row.get('join_tables', '').split(',') if t],
                        'write_table': remove_table_suffixes(row.get('write_table', '')),
                    }
            self._metadata_loaded = True
            logger.info(f"Loaded metadata for {len(self._metadata_cache)} write statements")
        except Exception as e:
            logger.error(f"Error loading write statement metadata cache: {e}", exc_info=True)

    def _get_write_statement_metadata(self, write_index: int) -> Dict[str, Any]:
        """
        Get write statement metadata from cache
        """
        if not self._metadata_loaded:
            self._load_write_metadata_cache()
        
        return self._metadata_cache.get(write_index, {})

    def _flatten_single_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Flatten a single result for CSV export"""
        flat_result = {
            'experiment_id': result.get('experiment_id', ''),
            'configuration': result.get('configuration', ''),
            'operation_type': result.get('operation_type', ''),
            'write_index': result.get('write_index', ''),
            'execution_time': result.get('execution_time', 0),
            'plan_execution_time': result.get('plan_execution_time', 0),
            'rows_affected': result.get('rows_affected', 0),
            'rows_inserted': result.get('rows_inserted', 0),
            'batch_mode': result.get('batch_mode', False),
            'batch_size': result.get('batch_size', 1),
            'error': result.get('error', ''),
            'statement': result.get('statement', '')[:200],  # Truncate for CSV
            'plan_total_cost': 0,
            'plan_actual_time': 0,
            'plan_node_type': '',
            'plan': '',
            'triggers': '',
            'query_type': '',
            'num_joins': 0,
            'num_scans': 0,
            'num_aggregations': 0,
            'start_table': '',
            'join_tables': '',
            'write_table': ''
        }
        
        # Add metadata if this is a write operation
        if flat_result['write_index'] != '':
            try:
                write_index = int(flat_result['write_index'])
                metadata = self._get_write_statement_metadata(write_index)
                flat_result.update(metadata)
            except (ValueError, TypeError):
                pass  # Skip invalid write_index
        
        # Add plan information if available
        if result.get('plan'):
            try:
                plan = result['plan']
                if isinstance(plan, dict):
                    flat_result['plan_total_cost'] = plan.get('Total Cost', 0)
                    flat_result['plan_actual_time'] = plan.get('Actual Total Time', 0)
                    flat_result['plan_node_type'] = plan.get('Node Type', '')
                    flat_result['plan'] = json.dumps(plan, ensure_ascii=False)
            except Exception as e:
                logger.warning(f"Error processing plan data: {e}")
        
        # Add triggers information
        if result.get('triggers'):
            try:
                flat_result['triggers'] = json.dumps(result.get('triggers', []), ensure_ascii=False)
            except Exception as e:
                logger.warning(f"Error processing triggers data: {e}")
        
        return flat_result

    def save_results_to_csv(self, filename: str = "benchmark_results.csv", append_mode: bool = False):
        """
        Save all results to a CSV file efficiently.
        If append_mode is True, only save new results since last save.
        """
        if not self.results:
            logger.warning("No results to save")
            return
        
        # Ensure metadata cache is loaded
        self._load_write_metadata_cache()
        
        # Define fieldnames once
        fieldnames = [
            'experiment_id', 'configuration', 'operation_type', 'write_index',
            'execution_time', 'plan_execution_time', 'rows_affected', 'rows_inserted',
            'batch_mode', 'batch_size', 'error', 'statement',
            'plan_total_cost', 'plan_actual_time', 'plan_node_type', 'plan', 'triggers',
            'query_type', 'num_joins', 'num_scans', 'num_aggregations',
            'start_table', 'join_tables', 'write_table'
        ]
        
        # Determine which results to save
        if append_mode and self._last_saved_index > 0:
            results_to_save = self.results[self._last_saved_index:]
            if not results_to_save:
                logger.debug("No new results to append")
                return
        else:
            results_to_save = self.results
            self._last_saved_index = 0
        
        # Write results to CSV
        try:
            mode = 'a' if append_mode and self._last_saved_index > 0 else 'w'
            write_header = mode == 'w'
            
            with open(filename, mode, newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                if write_header:
                    writer.writeheader()
                
                for result in results_to_save:
                    flat_result = self._flatten_single_result(result)
                    writer.writerow(flat_result)
            
            # Update the last saved index
            if append_mode:
                self._last_saved_index = len(self.results)
            
            action = "appended" if mode == 'a' else "saved"
            logger.info(f"Results {action} to {filename} ({len(results_to_save)} new rows, {len(self.results)} total)")
            
        except Exception as e:
            logger.error(f"Error saving results to CSV: {e}", exc_info=True)

    def close_connection(self):
        """Close database connection"""
        self.cleanup_views()
        
        # Use parent's close method
        super().close_connection()
    
    def batch_write_statements(self, write_statements: List[str]) -> str:
        """
        Combine multiple write statements into a single batch statement.
        Handles INSERT, UPDATE, and DELETE statements.
        """
        if not write_statements:
            return ""
        
        # Group statements by type
        inserts = []
        updates = []
        deletes = []
        
        for stmt in write_statements:
            stmt_upper = stmt.strip().upper()
            if stmt_upper.startswith('INSERT'):
                inserts.append(stmt)
            elif stmt_upper.startswith('UPDATE'):
                updates.append(stmt)
            elif stmt_upper.startswith('DELETE'):
                deletes.append(stmt)
        
        # Combine statements
        batched_statements = []
        
        # Batch INSERT statements
        if inserts:
            batched_insert = self._batch_insert_statements(inserts)
            if batched_insert:
                batched_statements.append(batched_insert)
        
        # Batch UPDATE statements
        if updates:
            batched_update = self._batch_update_statements(updates)
            if batched_update:
                batched_statements.append(batched_update)
        
        # Batch DELETE statements
        if deletes:
            batched_delete = self._batch_delete_statements(deletes)
            if batched_delete:
                batched_statements.append(batched_delete)
        
        # Join all batched statements
        return '; '.join(batched_statements)

    def _batch_insert_statements(self, insert_statements: List[str]) -> str:
        """
        Combine multiple INSERT statements into a single statement using UNION.
        Handles INSERT INTO ... SELECT DISTINCT ON ... ON CONFLICT (columns) DO NOTHING structure.
        """
        if not insert_statements:
            return ""
        
        try:
            # Parse the first INSERT to get the table and columns structure
            first_insert = insert_statements[0].strip()
            
            # Extract table name and columns from the first INSERT
            # Expected format: INSERT INTO table_name (columns) SELECT DISTINCT ON ...
            insert_parts = first_insert.split('SELECT DISTINCT ON', 1)
            if len(insert_parts) != 2:
                logger.warning("INSERT statement doesn't match expected format with SELECT DISTINCT ON")
                return '; '.join(insert_statements)  # Fallback to individual statements
            
            insert_part = insert_parts[0].strip()  # "INSERT INTO table_name (columns)"
            
            # Extract ON CONFLICT clause from the first statement
            on_conflict_pattern = r'ON CONFLICT\s*\([^)]+\)\s*DO\s+NOTHING'
            conflict_match = re.search(on_conflict_pattern, first_insert, re.IGNORECASE)
            on_conflict_clause = conflict_match.group(0) if conflict_match else "ON CONFLICT DO NOTHING"
            
            # Extract SELECT parts from all statements
            select_parts = []
            for stmt in insert_statements:
                stmt_parts = stmt.strip().split('SELECT DISTINCT ON', 1)
                if len(stmt_parts) == 2:
                    select_part = 'SELECT DISTINCT ON' + stmt_parts[1]
                    # Remove ON CONFLICT clause from the end if present
                    select_part = re.sub(on_conflict_pattern, '', select_part, flags=re.IGNORECASE).strip()
                    select_parts.append(select_part)
            
            if not select_parts:
                logger.warning("No valid SELECT parts found in INSERT statements")
                return '; '.join(insert_statements)
            
            # Combine SELECT parts with UNION
            combined_select = ' UNION '.join(select_parts)
            
            # Reconstruct the batched INSERT statement with proper ON CONFLICT clause
            batched_insert = f"{insert_part} {combined_select} {on_conflict_clause}"
            
            logger.debug(f"Batched {len(insert_statements)} INSERT statements")
            logger.warning(f"Batched INSERT statement: {batched_insert}")  # Log first 200 chars
            return batched_insert
            
        except Exception as e:
            logger.error(f"Error batching INSERT statements: {e}", exc_info=True)
            # Fallback to individual statements
            return '; '.join(insert_statements)

    def _batch_update_statements(self, update_statements: List[str]) -> str:
        """
        Combine multiple UPDATE statements. For now, just execute them sequentially.
        Future enhancement could use CTEs or other batching techniques.
        """
        if not update_statements:
            return ""
        
        logger.debug(f"Batching {len(update_statements)} UPDATE statements (sequential)")
        return '; '.join(update_statements)

    def _batch_delete_statements(self, delete_statements: List[str]) -> str:
        """
        Combine multiple DELETE statements. For now, just execute them sequentially.
        Future enhancement could use CTEs or other batching techniques.
        """
        if not delete_statements:
            return ""
        
        logger.debug(f"Batching {len(delete_statements)} DELETE statements (sequential)")
        return '; '.join(delete_statements)
