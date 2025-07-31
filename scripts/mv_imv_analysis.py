import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

def load_and_clean_data(csv_file):
    """Load and clean the benchmark results CSV file."""
    try:
        df = pd.read_csv(csv_file)
        
        # Strip whitespace from column names
        df.columns = df.columns.str.strip()
        
        # Convert numeric columns
        numeric_cols = ['execution_time', 'plan_execution_time', 'rows_affected']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        print(f"Loaded {len(df)} records from {csv_file}")
        print(f"Configurations: {df['configuration'].unique()}")
        print(f"Operation types: {df['operation_type'].unique()}")
        
        return df
        
    except FileNotFoundError:
        print(f"Error: File '{csv_file}' not found.")
        return None
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

def calculate_mv_total_time(df):
    """Calculate total execution time for MV setup including refresh operations."""
    mv_data = df[df['configuration'] == 'materialized_view'].copy()
    
    # Group by experiment_id and write_index
    mv_grouped = []
    
    for (exp_id, write_idx), group in mv_data.groupby(['experiment_id', 'write_index']):
        if pd.isna(write_idx):
            continue
            
        write_ops = group[group['operation_type'] == 'write']
        refresh_ops = group[group['operation_type'] == 'refresh']
        
        if len(write_ops) > 0:
            # Get write operation time
            write_time = write_ops['plan_execution_time'].iloc[0]
            
            # Get corresponding refresh time (should be same write_index)
            refresh_time = 0
            if len(refresh_ops) > 0:
                refresh_time = refresh_ops['execution_time'].iloc[0]
            
            # Total time is write + refresh
            total_time = write_time + refresh_time
            
            mv_grouped.append({
                'experiment_id': exp_id,
                'write_index': write_idx,
                'write_time': write_time,
                'refresh_time': refresh_time,
                'total_time': total_time
            })
    
    return pd.DataFrame(mv_grouped)

def calculate_immv_time(df):
    """Calculate execution time for IMMV setup."""
    immv_data = df[df['configuration'] == 'incremental_view'].copy()
    
    # Filter write operations only
    immv_writes = immv_data[immv_data['operation_type'] == 'write'].copy()
    
    immv_grouped = []
    for _, row in immv_writes.iterrows():
        if pd.notna(row['write_index']):
            immv_grouped.append({
                'experiment_id': row['experiment_id'],
                'write_index': row['write_index'],
                'immv_time': row['plan_execution_time']
            })
    
    return pd.DataFrame(immv_grouped)

def compute_speedup_analysis(df):
    """Compute speedup analysis between MV and IMMV setups."""
    
    # Calculate MV total times (write + refresh)
    mv_times = calculate_mv_total_time(df)
    
    # Calculate IMMV times
    immv_times = calculate_immv_time(df)
    
    # Merge the data
    merged = pd.merge(
        mv_times, 
        immv_times, 
        on=['experiment_id', 'write_index'],
        how='inner'
    )
    
    if len(merged) == 0:
        print("Warning: No matching records found between MV and IMMV setups")
        return None
    
    # Calculate speedup (MV_time / IMMV_time)
    merged['speedup'] = merged['total_time'] / merged['immv_time']
    
    # Calculate average speedup across all experiments for each write_index
    speedup_by_write_index = merged.groupby('write_index').agg({
        'speedup': ['mean', 'std', 'count'],
        'total_time': 'mean',
        'immv_time': 'mean'
    }).round(3)
    
    # Flatten column names
    speedup_by_write_index.columns = ['avg_speedup', 'std_speedup', 'count', 'avg_mv_time', 'avg_immv_time']
    speedup_by_write_index = speedup_by_write_index.reset_index()
    
    # Overall statistics
    overall_stats = {
        'total_comparisons': len(merged),
        'avg_speedup': merged['speedup'].mean(),
        'median_speedup': merged['speedup'].median(),
        'std_speedup': merged['speedup'].std(),
        'min_speedup': merged['speedup'].min(),
        'max_speedup': merged['speedup'].max()
    }
    
    return merged, speedup_by_write_index, overall_stats

def create_enhanced_visualizations(merged_data, speedup_by_write_index, overall_stats):
    """Create enhanced and clearer visualizations of the speedup analysis."""
    
    # Set up the plotting style
    plt.style.use('seaborn-v0_8-whitegrid')
    colors = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D']
    
    # Create figure with subplots
    fig = plt.figure(figsize=(20, 16))
    
    # Main title
    fig.suptitle('IMMV vs Materialized View Performance Analysis\nDetailed Trade-off Analysis', 
                 fontsize=20, fontweight='bold', y=0.98)
    
    # 1. Overall Performance Summary (Top section)
    gs = fig.add_gridspec(4, 4, height_ratios=[1, 1.2, 1.2, 1], width_ratios=[1, 1, 1, 1])
    
    # Summary statistics text box
    ax_summary = fig.add_subplot(gs[0, :])
    ax_summary.axis('off')
    
    summary_text = f"""
    Performance Summary: IMMV shows {overall_stats['avg_speedup']:.2f}x average speedup over Materialized Views
    • Total Comparisons: {overall_stats['total_comparisons']} | Median Speedup: {overall_stats['median_speedup']:.2f}x | Range: {overall_stats['min_speedup']:.2f}x - {overall_stats['max_speedup']:.2f}x
    • Interpretation: Values > 1.0 indicate IMMV is faster, values < 1.0 indicate MV is faster
    """
    
    ax_summary.text(0.5, 0.5, summary_text, transform=ax_summary.transAxes, 
                   fontsize=14, ha='center', va='center',
                   bbox=dict(boxstyle="round,pad=0.5", facecolor='lightblue', alpha=0.7))
    
    # 2. Speedup Distribution Analysis
    ax1 = fig.add_subplot(gs[1, :2])
    
    # Create histogram with better bins
    bins = np.logspace(np.log10(merged_data['speedup'].min()), 
                      np.log10(merged_data['speedup'].max()), 25)
    
    n, bins_edges, patches = ax1.hist(merged_data['speedup'], bins=bins, alpha=0.7, 
                                     color=colors[0], edgecolor='black', linewidth=0.5)
    
    # Color bars based on performance
    for i, patch in enumerate(patches):
        if bins_edges[i] < 1.0:
            patch.set_facecolor('#FF6B6B')  # Red for MV better
        else:
            patch.set_facecolor('#4ECDC4')  # Teal for IMMV better
    
    ax1.axvline(1.0, color='red', linestyle='--', linewidth=2, alpha=0.8, label='Equal Performance')
    ax1.axvline(overall_stats['avg_speedup'], color='blue', linestyle='-', linewidth=2, 
               label=f'Mean: {overall_stats["avg_speedup"]:.2f}x')
    ax1.axvline(overall_stats['median_speedup'], color='green', linestyle='-', linewidth=2,
               label=f'Median: {overall_stats["median_speedup"]:.2f}x')
    
    ax1.set_xscale('log')
    ax1.set_title('Speedup Distribution (Log Scale)\nRed: MV Better | Teal: IMMV Better', 
                 fontweight='bold', fontsize=14)
    ax1.set_xlabel('Speedup Factor (MV Time / IMMV Time)', fontsize=12)
    ax1.set_ylabel('Frequency', fontsize=12)
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # 3. Speedup Distribution by SQL Statement Type
    ax2 = fig.add_subplot(gs[1, 2:])
    
    # Create bins for speedup values instead of write indices
    speedup_bins = [0, 0.5, 1.0, 2.0, 5.0, float('inf')]
    speedup_labels = ['MV Much Better\n(<0.5x)', 'MV Better\n(0.5-1x)', 
                     'Balanced\n(1-2x)', 'IMMV Better\n(2-5x)', 'IMMV Much Better\n(>5x)']
    
    speedup_copy = speedup_by_write_index.copy()
    speedup_copy['performance_category'] = pd.cut(speedup_copy['avg_speedup'], 
                                                 bins=speedup_bins,
                                                 labels=speedup_labels,
                                                 include_lowest=True)
    
    category_counts = speedup_copy['performance_category'].value_counts().reindex(speedup_labels, fill_value=0)
    colors_perf = ['#FF4444', '#FF8888', '#FFDD44', '#88DD88', '#44AA44']
    
    bars = ax2.bar(range(len(category_counts)), category_counts.values, 
                  color=colors_perf, alpha=0.7, edgecolor='black', linewidth=0.5)
    
    ax2.set_title('Distribution of SQL Statements by Performance Category', fontweight='bold', fontsize=14)
    ax2.set_ylabel('Number of SQL Statements', fontsize=12)
    ax2.set_xlabel('Performance Category', fontsize=12)
    ax2.set_xticks(range(len(category_counts)))
    ax2.set_xticklabels(speedup_labels, rotation=0, ha='center')
    ax2.grid(True, alpha=0.3, axis='y')
    
    # Add value labels on bars
    for i, bar in enumerate(bars):
        height = bar.get_height()
        percentage = (height / len(speedup_by_write_index)) * 100
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}\n({percentage:.1f}%)', ha='center', va='bottom', 
                fontweight='bold', fontsize=10)
    
    # 4. Execution Time Comparison with Trend Analysis
    ax3 = fig.add_subplot(gs[2, :2])
    
    # Create scatter plot with better color mapping
    scatter = ax3.scatter(merged_data['immv_time'], merged_data['total_time'], 
                         c=merged_data['speedup'], cmap='RdYlBu_r', 
                         s=60, alpha=0.6, edgecolors='black', linewidth=0.5)
    
    # Add trend line in log space for better fit
    log_immv = np.log10(merged_data['immv_time'])
    log_mv = np.log10(merged_data['total_time'])
    z = np.polyfit(log_immv, log_mv, 1)
    
    # Create trend line in original space
    x_trend = np.logspace(np.log10(merged_data['immv_time'].min()), 
                         np.log10(merged_data['immv_time'].max()), 100)
    y_trend = 10**(z[0] * np.log10(x_trend) + z[1])
    ax3.plot(x_trend, y_trend, "r--", alpha=0.8, linewidth=2, label=f'Power Trend Line')
    
    # Equal performance line
    min_time = min(merged_data['immv_time'].min(), merged_data['total_time'].min())
    max_time = max(merged_data['immv_time'].max(), merged_data['total_time'].max())
    equal_line = np.logspace(np.log10(min_time), np.log10(max_time), 100)
    ax3.plot(equal_line, equal_line, 'k--', alpha=0.5, linewidth=2, label='Equal Performance')
    
    # Set log scales for both axes
    ax3.set_xscale('log')
    ax3.set_yscale('log')
    
    ax3.set_title('Execution Time Comparison with Trend Analysis (Log Scale)', fontweight='bold', fontsize=14)
    ax3.set_xlabel('IMMV Time (milliseconds, log scale)', fontsize=12)
    ax3.set_ylabel('MV Total Time (Write + Refresh, milliseconds, log scale)', fontsize=12)
    ax3.legend()
    ax3.grid(True, alpha=0.3, which='both')
    
    # Add colorbar
    cbar = plt.colorbar(scatter, ax=ax3)
    cbar.set_label('Speedup Factor', fontsize=12)
    
    # 5. Execution Time Analysis by Statement Complexity
    ax4 = fig.add_subplot(gs[2, 2:])
    
    # Analyze performance by execution time ranges (proxy for statement complexity)
    speedup_copy = speedup_by_write_index.copy()
    
    # Create complexity categories based on IMMV execution time
    immv_time_bins = [0, 1, 5, 20, 100, float('inf')]
    complexity_labels = ['Very Simple\n(<1ms)', 'Simple\n(1-5ms)', 
                        'Moderate\n(5-20ms)', 'Complex\n(20-100ms)', 'Very Complex\n(>100ms)']
    
    speedup_copy['complexity_category'] = pd.cut(speedup_copy['avg_immv_time'], 
                                               bins=immv_time_bins,
                                               labels=complexity_labels,
                                               include_lowest=True)
    
    complexity_stats = speedup_copy.groupby('complexity_category', observed=True).agg({
        'avg_speedup': ['mean', 'std', 'count'],
        'avg_mv_time': 'mean',
        'avg_immv_time': 'mean'
    })
    
    complexity_stats.columns = ['mean_speedup', 'std_speedup', 'count', 'mean_mv_time', 'mean_immv_time']
    complexity_stats = complexity_stats.reset_index()
    
    # Filter out categories with no data
    complexity_stats = complexity_stats[complexity_stats['count'] > 0]
    
    if len(complexity_stats) > 0:
        x = np.arange(len(complexity_stats))
        width = 0.35
        
        bars1 = ax4.bar(x - width/2, complexity_stats['mean_mv_time'], width, 
                       label='MV (Write + Refresh)', alpha=0.7, color=colors[0])
        bars2 = ax4.bar(x + width/2, complexity_stats['mean_immv_time'], width, 
                       label='IMMV (Write Only)', alpha=0.7, color=colors[2])
        
        ax4.set_title('Performance by Statement Complexity\n(Based on Execution Time)', fontweight='bold', fontsize=14)
        ax4.set_ylabel('Average Execution Time (milliseconds)', fontsize=12)
        ax4.set_xlabel('Statement Complexity Category', fontsize=12)
        ax4.set_xticks(x)
        ax4.set_xticklabels([cat for cat in complexity_stats['complexity_category']], rotation=0)
        ax4.legend()
        ax4.grid(True, alpha=0.3, axis='y')
        
        # Add speedup labels above bars
        for i, (bar1, bar2) in enumerate(zip(bars1, bars2)):
            speedup = complexity_stats.iloc[i]['mean_speedup']
            count = complexity_stats.iloc[i]['count']
            max_height = max(bar1.get_height(), bar2.get_height())
            ax4.text(i, max_height * 1.1, f'{speedup:.1f}x\n(n={count})', 
                    ha='center', va='bottom', fontweight='bold', fontsize=9,
                    bbox=dict(boxstyle="round,pad=0.3", facecolor='white', alpha=0.8))
    else:
        ax4.text(0.5, 0.5, 'No data available for complexity analysis', 
                transform=ax4.transAxes, ha='center', va='center', fontsize=12)
    
    # 6. Performance Insights Panel
    ax5 = fig.add_subplot(gs[3, :])
    ax5.axis('off')
    
    # Calculate insights
    immv_better_count = len(merged_data[merged_data['speedup'] > 1])
    mv_better_count = len(merged_data[merged_data['speedup'] < 1])
    immv_better_pct = (immv_better_count / len(merged_data)) * 100
    
    high_speedup_count = len(merged_data[merged_data['speedup'] > 5])
    low_speedup_count = len(merged_data[merged_data['speedup'] < 0.5])
    
    insights_text = f"""
    KEY INSIGHTS:
    • IMMV Performance: {immv_better_pct:.1f}% of SQL statements show IMMV outperforming MV ({immv_better_count}/{len(merged_data)} statements)
    • Significant Advantages: {high_speedup_count} statements show >5x IMMV speedup | {low_speedup_count} statements show >2x MV advantage
    • Performance Pattern: IMMV typically excels with simpler statements, MV better for complex operations requiring batch processing
    • SQL Workload Recommendation: Analyze your specific SQL statement patterns rather than relying on arbitrary line numbers
    """
    
    ax5.text(0.05, 0.5, insights_text, transform=ax5.transAxes, 
            fontsize=13, ha='left', va='center',
            bbox=dict(boxstyle="round,pad=0.5", facecolor='lightyellow', alpha=0.8))
    
    plt.tight_layout()
    
    # Save the enhanced plot
    output_file = '/app/data/results/enhanced_immv_vs_mv_analysis.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Enhanced visualization saved as '{output_file}'")
    
    plt.show()

def create_decision_matrix_plot(speedup_by_write_index, overall_stats):
    """Create a decision matrix visualization for choosing between MV and IMMV."""
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
    fig.suptitle('MV vs IMMV Decision Analysis', fontsize=16, fontweight='bold')
    
    # 1. Performance Distribution Matrix
    speedup_copy = speedup_by_write_index.copy()
    
    # Create execution time categories (proxy for statement complexity)
    speedup_copy['execution_time_category'] = pd.cut(speedup_copy['avg_immv_time'], 
                                                    bins=[0, 1, 5, 20, float('inf')],
                                                    labels=['Fast (<1ms)', 'Medium (1-5ms)', 
                                                           'Slow (5-20ms)', 'Very Slow (>20ms)'])
    
    speedup_copy['performance_category'] = pd.cut(speedup_copy['avg_speedup'], 
                                                 bins=[0, 0.5, 2, 5, float('inf')],
                                                 labels=['MV Much Better', 'MV/IMMV Similar', 
                                                        'IMMV Better', 'IMMV Much Better'])
    
    # Create heatmap data
    decision_matrix = speedup_copy.groupby(['execution_time_category', 'performance_category']).size().unstack(fill_value=0)
    
    if not decision_matrix.empty:
        sns.heatmap(decision_matrix, annot=True, fmt='d', cmap='RdYlBu_r', ax=ax1, 
                   cbar_kws={'label': 'Number of Statements'})
        ax1.set_title('Statement Performance by Execution Time', fontweight='bold')
        ax1.set_xlabel('Performance Outcome')
        ax1.set_ylabel('Statement Execution Time Category')
        ax1.tick_params(axis='x', rotation=45)
    else:
        ax1.text(0.5, 0.5, 'Insufficient data for matrix analysis', 
                transform=ax1.transAxes, ha='center', va='center')
    
    # 2. Speedup vs Execution Time Scatter
    x = speedup_by_write_index['avg_immv_time']
    y = speedup_by_write_index['avg_speedup']
    
    colors_map = ['red' if speedup < 0.5 else 'orange' if speedup < 1 else 'lightgreen' if speedup < 2 else 'darkgreen' 
                  for speedup in y]
    
    scatter = ax2.scatter(x, y, c=colors_map, s=50, alpha=0.7, edgecolors='black')
    
    # Add decision zones
    ax2.axhline(y=1, color='black', linestyle='-', alpha=0.8, label='Equal Performance')
    ax2.axhline(y=2, color='green', linestyle='--', alpha=0.6, label='Strong IMMV Preference')
    ax2.axhline(y=0.5, color='red', linestyle='--', alpha=0.6, label='Strong MV Preference')
    
    # Create x range for fill_between (using log scale)
    ax2.set_xscale('log')
    x_min, x_max = x.min() * 0.9, x.max() * 1.1
    
    ax2.fill_between([x_min, x_max], [2, 2], [100, 100], alpha=0.1, color='green', label='IMMV Recommended')
    ax2.fill_between([x_min, x_max], [0.01, 0.01], [0.5, 0.5], alpha=0.1, color='red', label='MV Recommended')
    ax2.fill_between([x_min, x_max], [0.5, 0.5], [2, 2], alpha=0.1, color='yellow', label='Context-Dependent')
    
    ax2.set_xlim(x_min, x_max)
    ax2.set_ylim(0.01, max(y.max() * 1.1, 10))
    ax2.set_title('Performance vs Statement Execution Time', fontweight='bold')
    ax2.set_xlabel('IMMV Execution Time (milliseconds, log scale)')
    ax2.set_ylabel('Speedup Factor (MV/IMMV)')
    ax2.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    # Save the decision matrix
    output_file = '/app/data/results/mv_immv_decision_matrix.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Decision matrix saved as '{output_file}'")
    
    plt.show()

def print_enhanced_summary_statistics(merged_data, speedup_by_write_index, overall_stats):
    """Print enhanced summary statistics with actionable insights."""
    
    print("\n" + "="*80)
    print("ENHANCED IMMV vs MATERIALIZED VIEW PERFORMANCE ANALYSIS")
    print("="*80)
    
    print(f"\nOVERALL PERFORMANCE METRICS:")
    print(f"{'Metric':<25} {'Value':<15} {'Interpretation'}")
    print("-" * 65)
    print(f"{'Total Comparisons':<25} {overall_stats['total_comparisons']:<15} {''}")
    print(f"{'Average Speedup':<25} {overall_stats['avg_speedup']:<15.3f} {'IMMV faster' if overall_stats['avg_speedup'] > 1 else 'MV faster'}")
    print(f"{'Median Speedup':<25} {overall_stats['median_speedup']:<15.3f} {'Typical case performance'}")
    print(f"{'Standard Deviation':<25} {overall_stats['std_speedup']:<15.3f} {'Performance variability'}")
    print(f"{'Min Speedup':<25} {overall_stats['min_speedup']:<15.3f} {'Worst case for IMMV'}")
    print(f"{'Max Speedup':<25} {overall_stats['max_speedup']:<15.3f} {'Best case for IMMV'}")
    
    # Performance distribution analysis
    immv_better = len(merged_data[merged_data['speedup'] > 1])
    mv_better = len(merged_data[merged_data['speedup'] < 1])
    equal_perf = len(merged_data[merged_data['speedup'] == 1])
    
    print(f"\nPERFORMANCE DISTRIBUTION:")
    print(f"• IMMV outperforms MV: {immv_better} cases ({immv_better/len(merged_data)*100:.1f}%)")
    print(f"• MV outperforms IMMV: {mv_better} cases ({mv_better/len(merged_data)*100:.1f}%)")
    print(f"• Equal performance: {equal_perf} cases ({equal_perf/len(merged_data)*100:.1f}%)")
    
    # Categorized analysis
    high_speedup = len(merged_data[merged_data['speedup'] > 5])
    moderate_speedup = len(merged_data[(merged_data['speedup'] > 2) & (merged_data['speedup'] <= 5)])
    low_speedup = len(merged_data[merged_data['speedup'] < 0.5])
    
    print(f"\nSPEEDUP INTENSITY ANALYSIS:")
    print(f"• High IMMV advantage (>5x): {high_speedup} cases ({high_speedup/len(merged_data)*100:.1f}%)")
    print(f"• Moderate IMMV advantage (2-5x): {moderate_speedup} cases ({moderate_speedup/len(merged_data)*100:.1f}%)")
    print(f"• Strong MV advantage (<0.5x): {low_speedup} cases ({low_speedup/len(merged_data)*100:.1f}%)")
    
    print(f"\nACTIONABLE RECOMMENDATIONS:")
    print("-" * 50)
    
    if overall_stats['avg_speedup'] > 2:
        print("✓ STRONG RECOMMENDATION: Implement IMMV for this workload type")
        print("  - Consistent significant performance improvements observed")
    elif overall_stats['avg_speedup'] > 1.2:
        print("✓ MODERATE RECOMMENDATION: Consider IMMV implementation")  
        print("  - Performance benefits likely, but evaluate specific use cases")
    elif overall_stats['avg_speedup'] < 0.8:
        print("✗ RECOMMENDATION: Stick with traditional Materialized Views")
        print("  - IMMV shows worse performance for this workload pattern")
    else:
        print("⚖ MIXED RESULTS: Performance depends on specific context")
        print("  - Detailed analysis needed for each use case")
    
    # Write index pattern analysis
    best_performers = speedup_by_write_index.nlargest(5, 'avg_speedup')
    worst_performers = speedup_by_write_index.nsmallest(5, 'avg_speedup')
    
    print(f"\nSQL STATEMENT PATTERN ANALYSIS:")
    print(f"Best IMMV Performance (Top 5 SQL Statements):")
    for _, row in best_performers.iterrows():
        print(f"  • Statement at line {int(row['write_index'])}: {row['avg_speedup']:.2f}x speedup")
    
    print(f"\nWorst IMMV Performance (Bottom 5 SQL Statements):")
    for _, row in worst_performers.iterrows():
        print(f"  • Statement at line {int(row['write_index'])}: {row['avg_speedup']:.2f}x speedup")

def main():
    """Main function to run the enhanced analysis."""
    
    # Configuration
    csv_file = '/app/data/benchmark_results.csv'
    
    print("ENHANCED IMMV vs MV Performance Analysis")
    print("="*60)
    
    # Load data
    df = load_and_clean_data(csv_file)
    if df is None:
        return
    
    # Compute speedup analysis
    print("\nComputing speedup analysis...")
    result = compute_speedup_analysis(df)
    
    if result is None:
        print("Analysis failed. Please check your data.")
        return
    
    merged_data, speedup_by_write_index, overall_stats = result
    
    # Print enhanced summary statistics
    print_enhanced_summary_statistics(merged_data, speedup_by_write_index, overall_stats)
    
    # Create enhanced visualizations
    print("\nCreating enhanced visualizations...")
    create_enhanced_visualizations(merged_data, speedup_by_write_index, overall_stats)
    
    # Create decision matrix
    print("\nCreating decision matrix...")
    create_decision_matrix_plot(speedup_by_write_index, overall_stats)
    
    # Save detailed results to CSV
    output_csv = '/app/data/results/enhanced_speedup_analysis_results.csv'
    speedup_by_write_index.to_csv(output_csv, index=False)
    print(f"\nDetailed results saved to '{output_csv}'")
    
    print("\nEnhanced analysis complete!")

if __name__ == "__main__":
    main()