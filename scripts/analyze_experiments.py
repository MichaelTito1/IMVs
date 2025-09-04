#!/usr/bin/env python3
"""
Experiment Results Analyzer for ML Advisor Data Collection

This script analyzes large-scale database experiment results comparing:
- Basic setup (no views)
- Materialized views
- Incrementally maintained materialized views (IMMVs)

Optimized for handling large JSON files (up to 1000KB, 40k+ lines).
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Generator
import warnings
import gc
import psutil
from datetime import datetime
import logging
from tqdm import tqdm
import ijson

warnings.filterwarnings('ignore')

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ExperimentAnalyzer:
    def __init__(self, data_directory: str = ".", chunk_size: int = 1000):
        """
        Initialize the analyzer with optimizations for large datasets.
        
        Args:
            data_directory: Directory containing experiment_*.json files
            chunk_size: Number of queries to process at once to manage memory
        """
        self.data_directory = Path(data_directory)
        self.chunk_size = chunk_size
        self.experiments_metadata = {}  # Store only metadata, not full data
        self.analysis_results = {}
        
    def get_memory_usage(self) -> str:
        """Get current memory usage."""
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024 / 1024
        return f"{memory_mb:.1f} MB"
    
    def discover_experiments(self) -> Dict[str, Dict[str, Path]]:
        """
        Discover experiment files without loading them into memory.
        
        Returns:
            Dictionary mapping experiment_id -> {setup_type: file_path}
        """
        pattern = "experiment_*_*.json"
        files = list(self.data_directory.glob(pattern))
        
        if not files:
            logger.error(f"No experiment files found in {self.data_directory}")
            return {}
            
        logger.info(f"Found {len(files)} experiment files")
        
        experiments = {}
        for file_path in files:
            try:
                filename = file_path.stem
                parts = filename.split('_')
                if len(parts) >= 3:
                    exp_idx = parts[1]
                    setup_type = '_'.join(parts[2:])
                    
                    if exp_idx not in experiments:
                        experiments[exp_idx] = {}
                    
                    experiments[exp_idx][setup_type] = file_path
                    
            except Exception as e:
                logger.warning(f"Error parsing filename {file_path}: {e}")
                continue
                
        logger.info(f"Discovered experiments for {len(experiments)} different select statements")
        return experiments
    
    def get_file_stats(self, file_path: Path) -> Dict:
        """Get basic statistics about a JSON file without fully loading it."""
        try:
            file_size = file_path.stat().st_size / 1024  # KB
            
            # Quick estimation of query count by streaming parse
            query_count = 0
            with open(file_path, 'rb') as f:
                parser = ijson.parse(f)
                for prefix, event, value in parser:
                    if prefix == 'query_list.item' and event == 'start_map':
                        query_count += 1
                    # Early exit after counting to avoid processing entire file
                    if query_count > 0 and prefix == 'database_stats':
                        break
            
            return {
                'file_size_kb': file_size,
                'estimated_query_count': query_count
            }
        except Exception as e:
            logger.warning(f"Error getting stats for {file_path}: {e}")
            return {'file_size_kb': 0, 'estimated_query_count': 0}
    
    def stream_parse_queries(self, file_path: Path) -> Generator[Dict, None, None]:
        """
        Stream parse queries from a large JSON file to avoid loading everything into memory.
        
        Args:
            file_path: Path to the JSON file
            
        Yields:
            Individual query dictionaries
        """
        try:
            with open(file_path, 'rb') as f:
                # Parse only the query_list array
                queries = ijson.items(f, 'query_list.item')
                for query in queries:
                    yield query
        except Exception as e:
            logger.error(f"Error streaming parse {file_path}: {e}")
            return
    
    def extract_execution_times_streaming(self) -> pd.DataFrame:
        """
        Extract execution times using streaming approach for memory efficiency.
        Filters out queries with zero_card=True or short_runtime=True.
        
        Returns:
            DataFrame with execution times, processed in chunks
        """
        experiments = self.discover_experiments()
        
        # Estimate total work for progress bar
        total_files = sum(len(setups) for setups in experiments.values())
        
        all_records = []
        filtered_counts = {'zero_card': 0, 'short_runtime': 0, 'timeout': 0, 'total_processed': 0}
        
        with tqdm(total=total_files, desc="Processing experiment files") as pbar:
            for exp_id, setups in experiments.items():
                for setup_type, file_path in setups.items():
                    pbar.set_description(f"Processing {exp_id}_{setup_type}")
                    
                    # Get file stats for monitoring
                    stats = self.get_file_stats(file_path)
                    logger.info(f"Processing {file_path.name}: "
                              f"{stats['file_size_kb']:.1f}KB, "
                              f"~{stats['estimated_query_count']} queries")
                    
                    # Process queries in chunks
                    chunk_records = []
                    stmt_idx = 0
                    
                    for query_data in self.stream_parse_queries(file_path):
                        filtered_counts['total_processed'] += 1
                        
                        # Check if query should be filtered out
                        zero_card = bool(query_data.get('zero_card', False))
                        short_runtime = bool(query_data.get('short_runtime', False))
                        timeout = bool(query_data.get('timeout', False))
                        
                        if zero_card:
                            filtered_counts['zero_card'] += 1
                            continue
                        
                        if short_runtime:
                            filtered_counts['short_runtime'] += 1
                            continue
                        
                        if timeout:
                            filtered_counts['timeout'] += 1
                            continue
                        
                        # Extract timing data
                        record = self.extract_single_query_timing(
                            query_data, exp_id, setup_type, stmt_idx)
                        
                        if record:
                            chunk_records.append(record)
                        
                        stmt_idx += 1
                        
                        # Process in chunks to manage memory
                        if len(chunk_records) >= self.chunk_size:
                            all_records.extend(chunk_records)
                            chunk_records = []
                            
                            # Force garbage collection
                            if len(all_records) % (self.chunk_size * 10) == 0:
                                gc.collect()
                                logger.info(f"Processed {len(all_records)} records, "
                                          f"Memory: {self.get_memory_usage()}")
                    
                    # Add remaining records from last chunk
                    all_records.extend(chunk_records)
                    pbar.update(1)
        
        # Log filtering statistics
        logger.info(f"Filtering summary:")
        logger.info(f"  Total queries processed: {filtered_counts['total_processed']:,}")
        logger.info(f"  Filtered out (zero_card): {filtered_counts['zero_card']:,}")
        logger.info(f"  Filtered out (short_runtime): {filtered_counts['short_runtime']:,}")
        logger.info(f"  Filtered out (timeout): {filtered_counts['timeout']:,}")
        logger.info(f"  Remaining for analysis: {len(all_records):,}")
        
        # Store filtering stats for later use
        self.filtering_stats = filtered_counts
        
        return pd.DataFrame(all_records)
    
    def extract_single_query_timing(self, query_data: Dict, exp_id: str, 
                                   setup_type: str, stmt_idx: int) -> Optional[Dict]:
        """
        Extract timing information from a single query.
        
        Args:
            query_data: Single query dictionary from JSON
            exp_id: Experiment ID
            setup_type: Setup type (none/materialized/incremental)
            stmt_idx: Statement index
            
        Returns:
            Dictionary with timing information or None if extraction fails
        """
        try:
            # Extract execution time from analyze_plans
            analyze_plans = query_data.get('analyze_plans', [])
            if not analyze_plans or len(analyze_plans) == 0:
                return None
                
            plan = analyze_plans[0].get('Plan', {})
            
            # Convert all numeric values to float to avoid Decimal/float mixing
            execution_time = float(plan.get('Actual Total Time', 0))
            planning_time = float(analyze_plans[0].get('Planning Time', 0))
            total_cost = float(plan.get('Total Cost', 0))
            startup_cost = float(plan.get('Startup Cost', 0))
            
            # Check flags
            timeout = bool(query_data.get('timeout', False))
            zero_card = bool(query_data.get('zero_card', False))
            short_runtime = bool(query_data.get('short_runtime', False))
            
            # Get SQL snippet for identification (truncated to save memory)
            sql_snippet = query_data.get('sql', '')[:100] + '...' if len(query_data.get('sql', '')) > 100 else query_data.get('sql', '')
            
            return {
                'experiment_id': str(exp_id),
                'setup_type': str(setup_type),
                'statement_idx': int(stmt_idx),
                'execution_time': execution_time,
                'planning_time': planning_time,
                'total_cost': total_cost,
                'startup_cost': startup_cost,
                'timeout': timeout,
                'zero_card': zero_card,
                'short_runtime': short_runtime,
                'sql_snippet': sql_snippet
            }
            
        except Exception as e:
            logger.warning(f"Error extracting timing for {exp_id}_{setup_type}_{stmt_idx}: {e}")
            return None
    
    def calculate_speedups_optimized(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate speedups with memory optimization for large datasets.
        
        Args:
            df: DataFrame with execution times
            
        Returns:
            DataFrame with speedup calculations
        """
        logger.info("Calculating speedups...")
        speedup_records = []
        
        # Group by experiment and statement, process in chunks
        grouped = df.groupby(['experiment_id', 'statement_idx'])
        
        chunk_count = 0
        for (exp_id, stmt_idx), group in tqdm(grouped, desc="Calculating speedups"):
            # Get execution times for each setup
            times = {}
            for _, row in group.iterrows():
                times[row['setup_type']] = row['execution_time']
            
            # Calculate speedups
            speedups = self.calculate_speedups_for_group(exp_id, stmt_idx, times)
            speedup_records.extend(speedups)
            
            chunk_count += 1
            if chunk_count % 1000 == 0:
                gc.collect()
                logger.info(f"Processed {chunk_count} groups, Memory: {self.get_memory_usage()}")
        
        return pd.DataFrame(speedup_records)
    
    def calculate_speedups_for_group(self, exp_id: str, stmt_idx: int, 
                                   times: Dict[str, float]) -> List[Dict]:
        """Calculate speedups for a single group of times."""
        speedups = []
        
        # Use basic setup as baseline if available
        if 'none' in times and times['none'] > 0:
            baseline = times['none']
            
            if 'materialized' in times and times['materialized'] > 0:
                speedup = baseline / times['materialized']
                speedups.append({
                    'experiment_id': exp_id,
                    'statement_idx': stmt_idx,
                    'comparison': 'materialized_vs_none',
                    'speedup': speedup,
                    'baseline_time': baseline,
                    'comparison_time': times['materialized']
                })
            
            if 'incremental' in times and times['incremental'] > 0:
                speedup = baseline / times['incremental']
                speedups.append({
                    'experiment_id': exp_id,
                    'statement_idx': stmt_idx,
                    'comparison': 'incremental_vs_none',
                    'speedup': speedup,
                    'baseline_time': baseline,
                    'comparison_time': times['incremental']
                })
        
        # Compare incremental vs materialized
        if 'materialized' in times and 'incremental' in times:
            if times['materialized'] > 0 and times['incremental'] > 0:
                speedup = times['materialized'] / times['incremental']
                speedups.append({
                    'experiment_id': exp_id,
                    'statement_idx': stmt_idx,
                    'comparison': 'incremental_vs_materialized',
                    'speedup': speedup,
                    'baseline_time': times['materialized'],
                    'comparison_time': times['incremental']
                })
        
        return speedups
    
    def analyze_results(self) -> Dict:
        """
        Perform comprehensive analysis with memory optimization.
        
        Returns:
            Dictionary containing analysis results
        """
        logger.info("Starting analysis...")
        start_time = datetime.now()
        
        # Extract execution times using streaming
        logger.info("Extracting execution times...")
        df_times = self.extract_execution_times_streaming()
        logger.info(f"Memory after extraction: {self.get_memory_usage()}")
        
        if df_times.empty:
            logger.error("No execution times extracted")
            return {}
        
        # Calculate speedups
        logger.info("Calculating speedups...")
        df_speedups = self.calculate_speedups_optimized(df_times)
        logger.info(f"Memory after speedup calculation: {self.get_memory_usage()}")
        
        # Calculate statistics efficiently
        logger.info("Calculating statistics...")
        stats = self.calculate_statistics_optimized(df_times, df_speedups)
        
        # Store results
        self.analysis_results = {
            'execution_times': df_times,
            'speedups': df_speedups,
            'statistics': stats
        }
        
        end_time = datetime.now()
        logger.info(f"Analysis completed in {(end_time - start_time).total_seconds():.1f} seconds")
        logger.info(f"Final memory usage: {self.get_memory_usage()}")
        
        return stats
    
    def calculate_statistics_optimized(self, df_times: pd.DataFrame, 
                                     df_speedups: pd.DataFrame) -> Dict:
        """Calculate statistics with memory optimization and proper type handling."""
        
        # Ensure numeric columns are properly typed
        numeric_columns = ['execution_time', 'planning_time', 'total_cost', 'startup_cost']
        for col in numeric_columns:
            if col in df_times.columns:
                df_times[col] = pd.to_numeric(df_times[col], errors='coerce')
        
        if not df_speedups.empty:
            df_speedups['speedup'] = pd.to_numeric(df_speedups['speedup'], errors='coerce')
            df_speedups['baseline_time'] = pd.to_numeric(df_speedups['baseline_time'], errors='coerce')
            df_speedups['comparison_time'] = pd.to_numeric(df_speedups['comparison_time'], errors='coerce')
        
        # Basic statistics using pandas built-in optimizations
        stats = {
            'total_experiments': df_times['experiment_id'].nunique(),
            'total_statements': len(df_times),
            'setup_types': df_times['setup_type'].unique().tolist(),
        }
        
        # Calculate aggregates efficiently
        time_stats = df_times.groupby('setup_type')['execution_time'].agg(['mean', 'median', 'std', 'count'])
        stats['avg_execution_times'] = time_stats['mean'].to_dict()
        stats['median_execution_times'] = time_stats['median'].to_dict()
        stats['std_execution_times'] = time_stats['std'].fillna(0).to_dict()  # Fill NaN with 0
        stats['count_by_setup'] = time_stats['count'].to_dict()
        
        # Speedup analysis
        if not df_speedups.empty:
            speedup_stats = {}
            for comparison in df_speedups['comparison'].unique():
                subset = df_speedups[df_speedups['comparison'] == comparison]
                
                # Filter out infinite and NaN speedups for statistics
                finite_mask = (
                    subset['speedup'].notna() &
                    (subset['speedup'] != float('inf')) & 
                    (subset['speedup'] != -float('inf')) &
                    (subset['speedup'] > 0)
                )
                finite_speedups = subset.loc[finite_mask, 'speedup']
                
                if len(finite_speedups) > 0:
                    speedup_stats[comparison] = {
                        'mean_speedup': finite_speedups.mean(),
                        'median_speedup': finite_speedups.median(),
                        'min_speedup': finite_speedups.min(),
                        'max_speedup': finite_speedups.max(),
                        'std_speedup': finite_speedups.std() if len(finite_speedups) > 1 else 0.0,
                        'count': len(finite_speedups),
                        'infinite_speedups': len(subset) - len(finite_speedups),
                        'percentile_95': finite_speedups.quantile(0.95) if len(finite_speedups) > 0 else 0.0,
                        'percentile_05': finite_speedups.quantile(0.05) if len(finite_speedups) > 0 else 0.0,
                    }
                else:
                    # Handle case where no finite speedups exist
                    speedup_stats[comparison] = {
                        'mean_speedup': 0.0,
                        'median_speedup': 0.0,
                        'min_speedup': 0.0,
                        'max_speedup': 0.0,
                        'std_speedup': 0.0,
                        'count': 0,
                        'infinite_speedups': len(subset),
                        'percentile_95': 0.0,
                        'percentile_05': 0.0,
                    }
            
            stats['speedup_analysis'] = speedup_stats
        
        return stats
    
    def print_summary(self) -> None:
        """Print a summary of the analysis results."""
        if not self.analysis_results:
            logger.error("No analysis results available. Please run analyze_results() first.")
            return
        
        stats = self.analysis_results['statistics']
        
        print("=" * 70)
        print("EXPERIMENT ANALYSIS SUMMARY")
        print("=" * 70)
        
        print(f"\nDataset Overview:")
        print(f"  Total experiments: {stats['total_experiments']:,}")
        print(f"  Total statements analyzed: {stats['total_statements']:,}")
        print(f"  Setup types: {', '.join(stats['setup_types'])}")
        
        print(f"\nExecution Time Statistics:")
        print(f"{'Setup Type':<20} {'Count':<10} {'Mean (ms)':<12} {'Median (ms)':<12} {'Std Dev (ms)':<12}")
        print("-" * 70)
        for setup in stats['setup_types']:
            count = stats['count_by_setup'].get(setup, 0)
            mean_time = stats['avg_execution_times'].get(setup, 0)
            median_time = stats['median_execution_times'].get(setup, 0)
            std_time = stats['std_execution_times'].get(setup, 0)
            print(f"{setup:<20} {count:<10,} {mean_time:<12.3f} {median_time:<12.3f} {std_time:<12.3f}")
        
        if 'speedup_analysis' in stats:
            print(f"\nSpeedup Analysis:")
            print("-" * 70)
            for comparison, data in stats['speedup_analysis'].items():
                print(f"\n  {comparison.replace('_', ' ').title()}:")
                print(f"    Sample size: {data['count']:,}")
                print(f"    Mean speedup: {data['mean_speedup']:.2f}x")
                print(f"    Median speedup: {data['median_speedup']:.2f}x")
                print(f"    95th percentile: {data['percentile_95']:.2f}x")
                print(f"    5th percentile: {data['percentile_05']:.2f}x")
                print(f"    Min/Max speedup: {data['min_speedup']:.2f}x / {data['max_speedup']:.2f}x")
                print(f"    Std deviation: {data['std_speedup']:.2f}")
                if data['infinite_speedups'] > 0:
                    print(f"    Infinite speedups: {data['infinite_speedups']:,}")
    
    def create_visualizations_optimized(self, output_dir: str = "/app/all_data/baseball/analysis/analysis_plots", 
                                      sample_size: int = 10000) -> None:
        """
        Create visualizations with sampling for large datasets.
        
        Args:
            output_dir: Directory to save plots
            sample_size: Maximum number of samples to use for plotting
        """
        if not self.analysis_results:
            logger.error("No analysis results available. Please run analyze_results() first.")
            return
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        df_times = self.analysis_results['execution_times']
        df_speedups = self.analysis_results['speedups']
        
        # Sample data for visualization if too large
        if len(df_times) > sample_size:
            logger.info(f"Sampling {sample_size} records for visualization from {len(df_times)} total")
            df_times_viz = df_times.sample(n=sample_size, random_state=42)
        else:
            df_times_viz = df_times
            
        if len(df_speedups) > sample_size:
            df_speedups_viz = df_speedups.sample(n=sample_size, random_state=42)
        else:
            df_speedups_viz = df_speedups
        
        # Set style
        plt.style.use('default')
        sns.set_palette("husl")
        
        # 1. Main execution analysis
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        
        # Box plot of execution times (log scale for better visibility)
        df_times_filtered = df_times_viz[df_times_viz['execution_time'] > 0]
        if not df_times_filtered.empty:
            sns.boxplot(data=df_times_filtered, x='setup_type', y='execution_time', ax=axes[0,0])
            axes[0,0].set_yscale('log')
            axes[0,0].set_title('Execution Time Distribution by Setup Type (Log Scale)')
            axes[0,0].set_ylabel('Execution Time (ms)')
            axes[0,0].tick_params(axis='x', rotation=45)
        
        # Average execution times
        avg_times = df_times.groupby('setup_type')['execution_time'].mean()
        avg_times.plot(kind='bar', ax=axes[0,1])
        axes[0,1].set_title('Average Execution Time by Setup Type')
        axes[0,1].set_ylabel('Average Execution Time (ms)')
        axes[0,1].tick_params(axis='x', rotation=45)
        
        # Speedup distribution
        if not df_speedups_viz.empty:
            finite_speedups = df_speedups_viz[
                (df_speedups_viz['speedup'] != float('inf')) & 
                (df_speedups_viz['speedup'] > 0) &
                (df_speedups_viz['speedup'] < 100)  # Filter extreme outliers
            ]
            if not finite_speedups.empty:
                sns.boxplot(data=finite_speedups, x='comparison', y='speedup', ax=axes[1,0])
                axes[1,0].set_title('Speedup Distribution (Finite Values Only)')
                axes[1,0].set_ylabel('Speedup Factor')
                axes[1,0].tick_params(axis='x', rotation=45)
        
        # Execution time vs planning time
        if not df_times_filtered.empty:
            axes[1,1].scatter(df_times_filtered['planning_time'], 
                            df_times_filtered['execution_time'], alpha=0.5)
            axes[1,1].set_xlabel('Planning Time (ms)')
            axes[1,1].set_ylabel('Execution Time (ms)')
            axes[1,1].set_title('Execution vs Planning Time')
            axes[1,1].set_xscale('log')
            axes[1,1].set_yscale('log')
        
        plt.tight_layout()
        plt.savefig(output_path / 'execution_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        logger.info(f"Visualizations saved to {output_path}")
        plt.close('all')  # Free memory
    
    def export_results_chunked(self, output_file: str = "/app/all_data/baseball/analysis/experiment_analysis.xlsx",
                             chunk_size: int = 50000) -> None:
        """
        Export results to Excel in chunks to handle large datasets.
        
        Args:
            output_file: Name of the output Excel file
            chunk_size: Number of rows to write at once
        """
        if not self.analysis_results:
            logger.error("No analysis results available. Please run analyze_results() first.")
            return
        
        logger.info(f"Exporting results to {output_file}")
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Export execution times in chunks
            df_times = self.analysis_results['execution_times']
            if len(df_times) > chunk_size:
                logger.info(f"Exporting {len(df_times)} execution time records in chunks")
                # Export first chunk with header, subsequent chunks without header
                for i, chunk in enumerate(pd.read_csv(pd.StringIO(df_times.to_csv(index=False)), chunksize=chunk_size)):
                    chunk.to_excel(writer, sheet_name='Execution_Times', 
                                 index=False, startrow=i*chunk_size, header=(i==0))
            else:
                df_times.to_excel(writer, sheet_name='Execution_Times', index=False)
            
            # Export speedups
            df_speedups = self.analysis_results['speedups']
            if not df_speedups.empty:
                if len(df_speedups) > chunk_size:
                    logger.info(f"Exporting {len(df_speedups)} speedup records in chunks")
                    # Similar chunked approach for speedups
                    for i, chunk in enumerate(pd.read_csv(pd.StringIO(df_speedups.to_csv(index=False)), chunksize=chunk_size)):
                        chunk.to_excel(writer, sheet_name='Speedups', 
                                     index=False, startrow=i*chunk_size, header=(i==0))
                else:
                    df_speedups.to_excel(writer, sheet_name='Speedups', index=False)
            
            # Export summary statistics
            stats = self.analysis_results['statistics']
            stats_records = []
            
            # Convert nested dict to flat records
            for setup_type in stats.get('setup_types', []):
                stats_records.append({
                    'Setup_Type': setup_type,
                    'Count': stats.get('count_by_setup', {}).get(setup_type, 0),
                    'Average_Time': stats.get('avg_execution_times', {}).get(setup_type, 0),
                    'Median_Time': stats.get('median_execution_times', {}).get(setup_type, 0),
                    'Std_Time': stats.get('std_execution_times', {}).get(setup_type, 0)
                })
            
            if stats_records:
                pd.DataFrame(stats_records).to_excel(writer, sheet_name='Summary_Stats', index=False)
        
        logger.info(f"Results exported to {output_file}")


def main():
    """Main function to run the analysis."""
    # Initialize analyzer with optimizations
    analyzer = ExperimentAnalyzer("/app/all_data/baseball/experiments", chunk_size=1000)
    
    # Run analysis
    logger.info("Starting experiment analysis...")
    results = analyzer.analyze_results()
    
    # Print summary
    analyzer.print_summary()
    
    # Create visualizations (with sampling for large datasets)
    analyzer.create_visualizations_optimized()
    
    # Export results
    analyzer.export_results_chunked()
    
    logger.info("Analysis complete!")


if __name__ == "__main__":
    main()