import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Constants
IMMV_SPEEDUP_THRESHOLD = 1.05  # IMMV is better if speedup > 1.05
MV_SPEEDUP_THRESHOLD = 0.95  # MV is better if speedup < 0.95
IMMV_OUTLIER_THRESHOLD = 5  # IMMV is considered an outlier if speedup > 5

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
                'total_mv_time': total_time
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

def get_merged_data(df):
    # Calculate MV total times (write + refresh)
    mv_times = calculate_mv_total_time(df)
    
    # Calculate IMMV times
    immv_times = calculate_immv_time(df)
    
    # Get rows_affected data from original dataframe for IMMV writes
    immv_cardinality = df[
        (df['configuration'] == 'incremental_view') & 
        (df['operation_type'] == 'write') &
        (df['write_index'].notna())
    ][['experiment_id', 'write_index', 'rows_affected']].copy()
    
    # Merge the data including cardinality information
    merged = pd.merge(
        mv_times, 
        immv_times, 
        on=['experiment_id', 'write_index'],
        how='inner'
    )
    
    # Add cardinality data
    merged = pd.merge(
        merged,
        immv_cardinality,
        on=['experiment_id', 'write_index'],
        how='left'
    )
    
    if len(merged) == 0:
        print("Warning: No matching records found between MV and IMMV setups")
        return None
    
    # Calculate speedup (MV_time / IMMV_time)
    merged['speedup'] = merged['total_mv_time'] / merged['immv_time']
    return merged

def compute_speedup_analysis(df):
    """Compute speedup analysis between MV and IMMV setups."""

    merged = get_merged_data(df)
    if merged is None:
        return None, None, None
    
    merged.to_csv('/app/data/results/merged_mv_immv_analysis.csv', index=False)

    # Calculate average speedup across all experiments for each write_index
    speedup_by_write_index = merged.groupby('write_index').agg({
        'speedup': ['mean', 'std', 'count'],
        'total_mv_time': 'mean',
        'immv_time': 'mean',
        'rows_affected': 'mean'
    }).round(3)
    
    # Flatten column names
    speedup_by_write_index.columns = ['avg_speedup', 'std_speedup', 'count', 'avg_mv_time', 'avg_immv_time', 'avg_rows_affected']
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

def create_enhanced_visualizations(merged_data, overall_stats):
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

    speedup_copy = merged_data.copy()
    speedup_copy['performance_category'] = pd.cut(speedup_copy['speedup'], 
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
        percentage = (height / len(speedup_copy)) * 100
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}\n({percentage:.1f}%)', ha='center', va='bottom', 
                fontweight='bold', fontsize=10)
    
    # 4. Execution Time Comparison with Trend Analysis
    ax3 = fig.add_subplot(gs[2, :2])
    
    # Create scatter plot with better color mapping
    scatter = ax3.scatter(merged_data['immv_time'], merged_data['total_mv_time'], 
                         c=merged_data['speedup'], cmap='RdYlBu_r', 
                         s=60, alpha=0.6, edgecolors='black', linewidth=0.5)
    
    # Add trend line in log space for better fit
    log_immv = np.log10(merged_data['immv_time'])
    log_mv = np.log10(merged_data['total_mv_time'])
    z = np.polyfit(log_immv, log_mv, 1)
    
    # Create trend line in original space
    x_trend = np.logspace(np.log10(merged_data['immv_time'].min()), 
                         np.log10(merged_data['immv_time'].max()), 100)
    y_trend = 10**(z[0] * np.log10(x_trend) + z[1])
    ax3.plot(x_trend, y_trend, "r--", alpha=0.8, linewidth=2, label=f'Power Trend Line')
    
    # Equal performance line
    min_time = min(merged_data['immv_time'].min(), merged_data['total_mv_time'].min())
    max_time = max(merged_data['immv_time'].max(), merged_data['total_mv_time'].max())
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
    speedup_copy = merged_data.copy()

    # Create complexity categories based on cardinality
    cardinality_bins = [0, 250, 500, 1000, 5000, float('inf')]
    complexity_labels = ['Very Simple\n(<250)', 'Simple\n(250-500)', 
                        'Moderate\n(500-1000)', 'Complex\n(1000-5000)', 'Very Complex\n(>5000)']
    
    speedup_copy['complexity_category'] = pd.cut(speedup_copy['rows_affected'], 
                                               bins=cardinality_bins,
                                               labels=complexity_labels,
                                               include_lowest=True)
    
    complexity_stats = speedup_copy.groupby('complexity_category', observed=True).agg({
        'speedup': ['mean', 'std', 'count'],
        'total_mv_time': 'mean',
        'immv_time': 'mean'
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

def create_cardinality_speedup_histogram(merged_data):
    """Create histogram showing relationship between rows_affected (cardinality) and speedup."""
    
    if 'rows_affected' not in merged_data.columns:
        print("Warning: 'rows_affected' column not found in data")
        return
    
    # Remove rows with missing or zero cardinality data and invalid speedup values
    data_clean = merged_data[
        (merged_data['rows_affected'].notna()) & 
        (merged_data['rows_affected'] > 0) &
        (merged_data['speedup'].notna()) &
        (np.isfinite(merged_data['speedup']))
    ].copy()
    
    if len(data_clean) == 0:
        print("Warning: No valid cardinality data available")
        return
    
    # Create cardinality bins
    cardinality_bins = [0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 2000, 10000, 100000, float('inf')]
    cardinality_labels = ['1-100', '101-200', '201-300', '301-400', '401-500', '501-600', '601-700', '701-800', '801-900', '901-1K', '1K-2K', '2K-10K', '10K-100K', '100K+']
    data_clean['cardinality_category'] = pd.cut(data_clean['rows_affected'], 
                                               bins=cardinality_bins, 
                                               labels=cardinality_labels, 
                                               right=False)
    
    # Calculate speedup statistics by cardinality category
    cardinality_stats = data_clean.groupby('cardinality_category').agg({
        'speedup': ['mean', 'std', 'count']
    }).round(3)
    cardinality_stats.columns = ['mean_speedup', 'std_speedup', 'count']
    cardinality_stats = cardinality_stats.reset_index()
    
    # Remove categories with no data or invalid values
    cardinality_stats = cardinality_stats[
        (cardinality_stats['count'] > 0) &
        (cardinality_stats['mean_speedup'].notna()) &
        (np.isfinite(cardinality_stats['mean_speedup']))
    ]
    
    if len(cardinality_stats) == 0:
        print("Warning: No valid cardinality statistics available for plotting")
        return
    
    # Replace NaN standard deviations with 0
    cardinality_stats['std_speedup'] = cardinality_stats['std_speedup'].fillna(0)
    
    # Create histogram
    fig, ax = plt.subplots(1, 1, figsize=(12, 8))
    
    bars = ax.bar(cardinality_stats['cardinality_category'], 
                  cardinality_stats['mean_speedup'],
                  yerr=cardinality_stats['std_speedup'], 
                  capsize=5, alpha=0.7,
                  color=['green' if x > 1 else 'red' for x in cardinality_stats['mean_speedup']],
                  edgecolor='black', linewidth=0.5)
    
    # Add reference line and labels
    ax.axhline(y=1, color='red', linestyle='--', alpha=0.7, label='Equal Performance')
    ax.set_title('IMMV vs MV Performance by Data Cardinality\n(Rows Affected per Write Operation)', 
                fontweight='bold', fontsize=14)
    ax.set_xlabel('Rows Affected per Write', fontsize=12)
    ax.set_ylabel('Average Speedup Factor (MV Time / IMMV Time)', fontsize=12)
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    # Add value and count labels on bars
    for i, bar in enumerate(bars):
        height = bar.get_height()
        count = cardinality_stats.iloc[i]['count']
        # Ensure height is finite before plotting text
        if np.isfinite(height):
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{height:.2f}x\n(n={count})', 
                   ha='center', va='bottom', fontweight='bold', fontsize=10)
    
    plt.tight_layout()
    
    # Save the plot
    output_file = '/app/data/results/cardinality_speedup_histogram.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Cardinality histogram saved as '{output_file}'")
    
    plt.show()
    
    # Print summary statistics
    print(f"\nCardinality vs Speedup Analysis:")
    print(f"Note: Error bars represent ±1 standard deviation showing variability within each category")
    print(f"{'Cardinality':<15} {'Avg Speedup':<12} {'Std Dev':<10} {'Count':<8} {'Recommendation'}")
    print("-" * 70)
    for _, row in cardinality_stats.iterrows():
        recommendation = "IMMV" if row['mean_speedup'] > 1.2 else "MV" if row['mean_speedup'] < 0.8 else "Mixed"
        print(f"{row['cardinality_category']:<15} {row['mean_speedup']:<12.2f} ±{row['std_speedup']:<9.2f} {row['count']:<8} {recommendation}")

def print_enhanced_summary_statistics(merged_data, overall_stats):
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
    immv_better = len(merged_data[merged_data['speedup'] > IMMV_SPEEDUP_THRESHOLD])
    mv_better = len(merged_data[merged_data['speedup'] < MV_SPEEDUP_THRESHOLD])
    similar_perf = len(merged_data[(merged_data['speedup'] >= MV_SPEEDUP_THRESHOLD) & (merged_data['speedup'] <= IMMV_SPEEDUP_THRESHOLD)])
    
    print(f"\nPERFORMANCE DISTRIBUTION:")
    print(f"• IMMV outperforms MV: {immv_better} cases ({immv_better/len(merged_data)*100:.1f}%)")
    print(f"• MV outperforms IMMV: {mv_better} cases ({mv_better/len(merged_data)*100:.1f}%)")
    print(f"• Similar performance: {similar_perf} cases ({similar_perf/len(merged_data)*100:.1f}%)")
    
    # Categorized analysis
    high_speedup = len(merged_data[merged_data['speedup'] > 5])
    moderate_speedup = len(merged_data[(merged_data['speedup'] > IMMV_SPEEDUP_THRESHOLD) & (merged_data['speedup'] < IMMV_SPEEDUP_THRESHOLD)])
    low_speedup = len(merged_data[merged_data['speedup'] < MV_SPEEDUP_THRESHOLD])
    
    print(f"\nSPEEDUP INTENSITY ANALYSIS:")
    print(f"• High IMMV advantage (>5x): {high_speedup} cases ({high_speedup/len(merged_data)*100:.1f}%)")
    print(f"• Moderate IMMV advantage (1.05-5x): {moderate_speedup} cases ({moderate_speedup/len(merged_data)*100:.1f}%)")
    print(f"• Strong MV advantage (<0.95x): {low_speedup} cases ({low_speedup/len(merged_data)*100:.1f}%)")

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
    best_performers = merged_data.nlargest(5, 'speedup')
    worst_performers = merged_data.nsmallest(5, 'speedup')
    
    print(f"\nSQL STATEMENT PATTERN ANALYSIS:")
    print(f"Best IMMV Performance (Top 5 SQL Statements):")
    for _, row in best_performers.iterrows():
        print(f"  • Statement at line {int(row['write_index'])}: {row['speedup']:.2f}x speedup")
    
    print(f"\nWorst IMMV Performance (Bottom 5 SQL Statements):")
    for _, row in worst_performers.iterrows():
        print(f"  • Statement at line {int(row['write_index'])}: {row['speedup']:.2f}x speedup")

def create_cardinality_speedup_scatter_plot(merged_data):
    """Create scatter plot showing relationship between cardinality and speedup with trend analysis."""
    
    if 'rows_affected' not in merged_data.columns:
        print("Warning: 'rows_affected' column not found in data")
        return
    
    # Remove rows with missing or zero cardinality data and invalid speedup values
    data_clean = merged_data[
        (merged_data['rows_affected'].notna()) & 
        (merged_data['rows_affected'] > 0) &
        (merged_data['speedup'].notna()) &
        (np.isfinite(merged_data['speedup']))
    ].copy()
    
    if len(data_clean) == 0:
        print("Warning: No valid cardinality data available")
        return
    
    # Create figure with subplots
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Cardinality vs Performance Analysis: MV vs IMMV Trade-offs', fontsize=16, fontweight='bold')
    
    # 1. Scatter plot: Cardinality vs Speedup (Linear scale) - disregard outliers
    x = data_clean[data_clean['speedup'] < IMMV_OUTLIER_THRESHOLD]['rows_affected']
    y = data_clean[data_clean['speedup'] < IMMV_OUTLIER_THRESHOLD]['speedup']

    # Color points based on performance advantage
    colors = ['red' if s < 0.8 else 'orange' if s < 1 else 'lightgreen' if s < 3 else 'darkgreen' 
              for s in y]
    
    scatter = ax1.scatter(x, y, c=colors, alpha=0.6, s=50, edgecolors='black', linewidth=0.5)
    
    # Add trend line
    if len(data_clean) > 5:
        z = np.polyfit(x, y, 1)
        x_trend = np.linspace(x.min(), x.max(), 100)
        y_trend = z[0] * x_trend + z[1]
        ax1.plot(x_trend, y_trend, "r--", alpha=0.8, linewidth=2, 
                label=f'Linear trend: slope={z[0]:.2e}')
    
    # Add reference lines
    ax1.axhline(y=1, color='black', linestyle='-', alpha=0.7, label='Equal Performance')
    ax1.axhline(y=2, color='green', linestyle='--', alpha=0.5, label='2x IMMV Advantage')
    ax1.axhline(y=0.5, color='red', linestyle='--', alpha=0.5, label='2x MV Advantage')
    
    ax1.set_xlabel('Rows Affected')
    ax1.set_ylabel('Speedup Factor')
    ax1.set_title('Cardinality vs Speedup Relationship')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # 2. Box plot: Speedup distribution by cardinality ranges
    # Create cardinality bins
    cardinality_bins = [0, 100, 500, 1000, 5000, float('inf')]
    cardinality_labels = ['1-100', '101-500', '501-1K', '1K-5K', '5K+']
    
    data_clean['cardinality_category'] = pd.cut(data_clean['rows_affected'], 
                                               bins=cardinality_bins, 
                                               labels=cardinality_labels, 
                                               right=False)
    
    # Create box plot data
    box_data = []
    box_labels = []
    for cat in cardinality_labels:
        cat_data = data_clean[(data_clean['cardinality_category'] == cat) & (data_clean['speedup'] < IMMV_OUTLIER_THRESHOLD)]['speedup']
        if len(cat_data) > 0:
            box_data.append(cat_data)
            box_labels.append(f'{cat}\n(n={len(cat_data)})')
    
    if box_data:
        box_plot = ax2.boxplot(box_data, labels=box_labels, patch_artist=True)
        
        # Color boxes based on median performance
        for i, box in enumerate(box_plot['boxes']):
            median_speedup = np.median(box_data[i])
            if median_speedup > 2:
                box.set_facecolor('lightgreen')
            elif median_speedup > 1.2:
                box.set_facecolor('lightblue')
            elif median_speedup < 0.8:
                box.set_facecolor('lightcoral')
            else:
                box.set_facecolor('lightyellow')
        
        ax2.axhline(y=1, color='black', linestyle='-', alpha=0.7, label='Equal Performance')
        ax2.set_ylabel('Speedup Factor')
        ax2.set_xlabel('Cardinality Range')
        ax2.set_title('Speedup Distribution by Cardinality')
        ax2.legend()
        ax2.grid(True, alpha=0.3, axis='y')
    
    # 3. Performance regions heatmap
    # Create 2D bins for cardinality and speedup
    cardinality_edges = np.logspace(0, np.log10(data_clean['rows_affected'].max()), 10)
    speedup_edges = np.logspace(np.log10(0.1), np.log10(max(data_clean['speedup'].max(), 10)), 10)
    
    H, xedges, yedges = np.histogram2d(data_clean['rows_affected'], data_clean['speedup'], 
                                      bins=[cardinality_edges, speedup_edges])
    
    # Create heatmap
    im = ax3.imshow(H.T, origin='lower', aspect='auto', cmap='YlOrRd', 
                   extent=[cardinality_edges[0], cardinality_edges[-1], 
                          speedup_edges[0], speedup_edges[-1]])
    
    ax3.set_xscale('log')
    ax3.set_yscale('log')
    ax3.axhline(y=1, color='white', linestyle='-', linewidth=2, alpha=0.8)
    ax3.set_xlabel('Rows Affected (log scale)')
    ax3.set_ylabel('Speedup Factor (log scale)')
    ax3.set_title('Performance Density Map')
    
    # Add colorbar
    cbar = plt.colorbar(im, ax=ax3)
    cbar.set_label('Number of Data Points')
    
    # 4. Performance advantage analysis
    # Calculate percentage of IMMV advantage vs cardinality
    advantage_data = []
    cardinality_ranges = []
    
    for i, cat in enumerate(cardinality_labels):
        cat_data = data_clean[data_clean['cardinality_category'] == cat]
        if len(cat_data) > 0:
            immv_better = len(cat_data[cat_data['speedup'] > 1.05])
            mv_better = len(cat_data[cat_data['speedup'] < 0.95])
            similar = len(cat_data) - immv_better - mv_better;
            
            total = len(cat_data);
            advantage_data.append([
                (immv_better / total) * 100,
                (similar / total) * 100, 
                (mv_better / total) * 100
            ])
            cardinality_ranges.append(cat)
    
    if advantage_data:
        advantage_array = np.array(advantage_data)
        
        # Create stacked bar chart
        width = 0.6
        x_pos = np.arange(len(cardinality_ranges))

        p1 = ax4.bar(x_pos, advantage_array[:, 0], width, label='IMMV Better (>1.05x)', color="#4CAF50", alpha=0.7)
        p2 = ax4.bar(x_pos, advantage_array[:, 1], width, bottom=advantage_array[:, 0], 
                    label='Similar (0.95-1.05x)', color="#D3D304", alpha=0.7)
        p3 = ax4.bar(x_pos, advantage_array[:, 2], width, 
                    bottom=advantage_array[:, 0] + advantage_array[:, 1],
                    label='MV Better (<0.95x)', color="#C23212", alpha=0.7)

        ax4.set_ylabel('Percentage of Write Operations (%)')
        ax4.set_xlabel('Cardinality Range')
        ax4.set_title('Performance Advantage Distribution')
        ax4.set_xticks(x_pos)
        ax4.set_xticklabels(cardinality_ranges, rotation=45)
        ax4.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        ax4.grid(True, alpha=0.3, axis='y')
        
        # Add percentage labels on bars
        for i, (immv, similar, mv) in enumerate(advantage_array):
            if immv > 5:
                ax4.text(i, immv/2, f'{immv:.0f}%', ha='center', va='center', fontweight='bold')
            if similar > 5:
                ax4.text(i, immv + similar/2, f'{similar:.0f}%', ha='center', va='center', fontweight='bold')
            if mv > 5:
                ax4.text(i, immv + similar + mv/2, f'{mv:.0f}%', ha='center', va='center', fontweight='bold')
    
    plt.tight_layout()
    
    # Save the plot
    output_file = '/app/data/results/cardinality_speedup_analysis.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Cardinality vs speedup analysis saved as '{output_file}'")
    
    plt.show()
    
    # Print detailed analysis
    print(f"\nCARDINALITY vs SPEEDUP ANALYSIS:")
    print("="*60)
    
    # Calculate correlation
    correlation = np.corrcoef(np.log10(data_clean['rows_affected']), 
                             np.log10(data_clean['speedup']))[0,1]
    print(f"Log-scale correlation between cardinality and speedup: {correlation:.3f}")
    
    if abs(correlation) > 0.3:
        trend = "negative" if correlation < 0 else "positive"
        strength = "strong" if abs(correlation) > 0.6 else "moderate"
        print(f"There is a {strength} {trend} correlation between cardinality and speedup.")
    else:
        print("There is weak correlation between cardinality and speedup.")
    
    # Analyze performance by cardinality ranges
    print(f"\nPerformance by Cardinality Ranges:")
    print(f"{'Range':<12} {'Count':<8} {'Median Speedup':<15} {'IMMV Better':<12} {'MV Better':<10} {'Recommendation'}")
    print("-" * 85)
    
    for cat in cardinality_labels:
        cat_data = data_clean[data_clean['cardinality_category'] == cat]
        if len(cat_data) > 0:
            median_speedup = cat_data['speedup'].median()
            immv_better_pct = (len(cat_data[cat_data['speedup'] > 1.05]) / len(cat_data)) * 100
            mv_better_pct = (len(cat_data[cat_data['speedup'] < 0.95]) / len(cat_data)) * 100

            if median_speedup > 1.05:
                recommendation = "IMMV"
            elif median_speedup < 0.95:
                recommendation = "MV"
            else:
                recommendation = "Mixed"
            
            print(f"{cat:<12} {len(cat_data):<8} {median_speedup:<15.2f} %{immv_better_pct:<12.1f}% {mv_better_pct:<10.1f} {recommendation}")

def create_cardinality_tradeoff_analysis(merged_data):
    """Create detailed tradeoff analysis focusing on cardinality thresholds."""
    
    if 'rows_affected' not in merged_data.columns:
        print("Warning: 'rows_affected' column not found in data")
        return
    
    # Clean data
    data_clean = merged_data[
        (merged_data['rows_affected'].notna()) & 
        (merged_data['rows_affected'] > 0) &
        (merged_data['speedup'].notna()) &
        (np.isfinite(merged_data['speedup']))
    ].copy()
    
    if len(data_clean) == 0:
        print("Warning: No valid cardinality data available")
        return
    
    # Create figure
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
    fig.suptitle('Cardinality-Based Decision Framework: When to Choose MV vs IMMV', 
                 fontsize=16, fontweight='bold')
    
    # 1. Decision threshold analysis
    cardinality_thresholds = np.logspace(0, np.log10(data_clean['rows_affected'].max()), 20)
    immv_advantage_percentages = []
    mv_advantage_percentages = []
    sample_counts = []
    
    for threshold in cardinality_thresholds:
        subset = data_clean[data_clean['rows_affected'] <= threshold]
        if len(subset) > 5:  # Minimum sample size
            immv_better = len(subset[subset['speedup'] > 1.2])
            mv_better = len(subset[subset['speedup'] < 0.8])
            
            immv_advantage_percentages.append((immv_better / len(subset)) * 100)
            mv_advantage_percentages.append((mv_better / len(subset)) * 100)
            sample_counts.append(len(subset))
        else:
            immv_advantage_percentages.append(np.nan)
            mv_advantage_percentages.append(np.nan)
            sample_counts.append(0)
    
    # Plot decision curves
    valid_indices = ~np.isnan(immv_advantage_percentages)
    valid_thresholds = cardinality_thresholds[valid_indices]
    valid_immv = np.array(immv_advantage_percentages)[valid_indices]
    valid_mv = np.array(mv_advantage_percentages)[valid_indices]
    
    ax1.plot(valid_thresholds, valid_immv, 'g-', linewidth=2, label='IMMV Advantage %', marker='o', markersize=4)
    ax1.plot(valid_thresholds, valid_mv, 'r-', linewidth=2, label='MV Advantage %', marker='s', markersize=4)
    ax1.axhline(y=50, color='black', linestyle='--', alpha=0.7, label='50% Threshold')
    
    # Find crossover point
    if len(valid_immv) > 5 and len(valid_mv) > 5:
        # Find where IMMV advantage crosses below MV advantage
        for i in range(1, len(valid_thresholds)):
            if valid_immv[i-1] > valid_mv[i-1] and valid_immv[i] < valid_mv[i]:
                crossover_point = valid_thresholds[i]
                ax1.axvline(x=crossover_point, color='purple', linestyle=':', linewidth=2,
                           label=f'Crossover: {crossover_point:.0f} rows')
                break
    
    ax1.set_xscale('log')
    ax1.set_xlabel('Cardinality Threshold (rows affected)')
    ax1.set_ylabel('Percentage of Statements with Advantage')
    ax1.set_title('Performance Advantage vs Cardinality Threshold')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # 2. Cost-benefit matrix
    # Define cardinality ranges and calculate metrics
    ranges = [
        (1, 10, 'Very Low'),
        (11, 100, 'Low'), 
        (101, 500, 'Medium'),
        (501, 2000, 'High'),
        (2001, float('inf'), 'Very High')
    ]
    
    matrix_data = []
    range_labels = []
    
    for min_card, max_card, label in ranges:
        if max_card == float('inf'):
            subset = data_clean[data_clean['rows_affected'] >= min_card]
        else:
            subset = data_clean[
                (data_clean['rows_affected'] >= min_card) & 
                (data_clean['rows_affected'] <= max_card)
            ]
        
        if len(subset) > 0:
            avg_speedup = subset['speedup'].mean()
            median_speedup = subset['speedup'].median()
            immv_win_rate = (len(subset[subset['speedup'] > 1.2]) / len(subset)) * 100
            mv_win_rate = (len(subset[subset['speedup'] < 0.8]) / len(subset)) * 100
            count = len(subset)
            
            matrix_data.append([avg_speedup, median_speedup, immv_win_rate, mv_win_rate, count])
            range_labels.append(f'{label}\n({min_card}-{max_card if max_card != float("inf") else "∞"})')
    
    if matrix_data:
        matrix_array = np.array(matrix_data)
        
        # Create heatmap for different metrics
        metrics = ['Avg Speedup', 'Median Speedup', 'IMMV Win %', 'MV Win %', 'Sample Count']
        
        # Normalize data for heatmap (each column separately)
        normalized_data = matrix_array.copy()
        for i in range(matrix_array.shape[1]):
            col_data = matrix_array[:, i]
            if col_data.max() != col_data.min():
                if i < 2:  # For speedup metrics, center around 1
                    normalized_data[:, i] = (col_data - 1) / max(abs(col_data - 1).max(), 0.1)
                else:  # For percentages and counts, normalize to 0-1
                    normalized_data[:, i] = (col_data - col_data.min()) / (col_data.max() - col_data.min())
        
        im = ax2.imshow(normalized_data.T, cmap='RdYlGn', aspect='auto', vmin=-1, vmax=1)
        
        # Add text annotations
        for i in range(len(range_labels)):
            for j in range(len(metrics)):
                if j < 2:  # Speedup metrics
                    text = f'{matrix_array[i, j]:.2f}'
                elif j < 4:  # Percentage metrics
                    text = f'{matrix_array[i, j]:.0f}%'
                else:  # Count
                    text = f'{int(matrix_array[i, j])}'
                
                color = 'white' if abs(normalized_data[i, j]) > 0.5 else 'black'
                ax2.text(i, j, text, ha='center', va='center', color=color, fontweight='bold')
        
        ax2.set_xticks(range(len(range_labels)))
        ax2.set_xticklabels(range_labels)
        ax2.set_yticks(range(len(metrics)))
        ax2.set_yticklabels(metrics)
        ax2.set_xlabel('Cardinality Range')
        ax2.set_title('Performance Metrics by Cardinality Range')
        
        # Add colorbar
        cbar = plt.colorbar(im, ax=ax2)
        cbar.set_label('Normalized Performance (Green=Better, Red=Worse)')
    
    plt.tight_layout()
    
    # Save the plot
    output_file = '/app/data/results/cardinality_tradeoff_analysis.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Cardinality tradeoff analysis saved as '{output_file}'")
    
    plt.show()
    
    # Print decision recommendations
    print(f"\nCARDINALITY-BASED DECISION RECOMMENDATIONS:")
    print("="*70)
    
    for i, (min_card, max_card, label) in enumerate(ranges):
        if i < len(matrix_data):
            data_row = matrix_data[i]
            avg_speedup, median_speedup, immv_win_rate, mv_win_rate, count = data_row
            
            print(f"\n{label.upper()} CARDINALITY ({min_card}-{max_card if max_card != float('inf') else '∞'} rows):")
            print(f"  • Sample size: {int(count)} statements")
            print(f"  • Average speedup: {avg_speedup:.2f}x")
            print(f"  • IMMV advantage rate: {immv_win_rate:.1f}%")
            print(f"  • MV advantage rate: {mv_win_rate:.1f}%")
            
            if immv_win_rate > 60:
                recommendation = "STRONG IMMV RECOMMENDATION"
            elif immv_win_rate > 40:
                recommendation = "MODERATE IMMV PREFERENCE"
            elif mv_win_rate > 60:
                recommendation = "STRONG MV RECOMMENDATION"
            elif mv_win_rate > 40:
                recommendation = "MODERATE MV PREFERENCE"
            else:
                recommendation = "MIXED - CONTEXT DEPENDENT"
            
            print(f"  • RECOMMENDATION: {recommendation}")

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
    print_enhanced_summary_statistics(merged_data, overall_stats)
    
    # Create enhanced visualizations
    print("\nCreating enhanced visualizations...")
    create_enhanced_visualizations(merged_data, overall_stats)
    
    # Create decision matrix
    print("\nCreating decision matrix...")
    create_decision_matrix_plot(speedup_by_write_index, overall_stats)

    # Create original cardinality vs speedup histogram
    print("\nCreating cardinality vs speedup histogram...")
    create_cardinality_speedup_histogram(merged_data)
    
    # Create new cardinality analysis visualizations
    print("\nCreating comprehensive cardinality vs speedup analysis...")
    create_cardinality_speedup_scatter_plot(merged_data)
    
    print("\nCreating cardinality-based tradeoff analysis...")
    create_cardinality_tradeoff_analysis(merged_data)
    
    # Save detailed results to CSV
    output_csv = '/app/data/results/enhanced_speedup_analysis_results.csv'
    speedup_by_write_index.to_csv(output_csv, index=False)
    print(f"\nDetailed results saved to '{output_csv}'")
    
    print("\nEnhanced analysis complete!")

if __name__ == "__main__":
    main()