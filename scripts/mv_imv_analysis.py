import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
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

def create_visualizations(merged_data, speedup_by_write_index, overall_stats):
    """Create comprehensive visualizations of the speedup analysis."""
    
    # Set up the plotting style
    plt.style.use('default')
    sns.set_palette("husl")
    
    # Create figure with subplots
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('IMMV vs Materialized View Performance Analysis', fontsize=16, fontweight='bold')
    
    # 1. Speedup by Write Index (Bar Chart)
    ax1 = axes[0, 0]
    bars = ax1.bar(speedup_by_write_index['write_index'], 
                   speedup_by_write_index['avg_speedup'],
                   yerr=speedup_by_write_index['std_speedup'],
                   capsize=5, alpha=0.7, color='skyblue', edgecolor='navy')
    ax1.set_title('Average Speedup by Write Index\n(MV Time / IMMV Time)', fontweight='bold')
    ax1.set_xlabel('Write Index')
    ax1.set_ylabel('Speedup Factor')
    ax1.grid(True, alpha=0.3)
    ax1.axhline(y=1, color='red', linestyle='--', alpha=0.7, label='No Speedup (1.0)')
    ax1.legend()
    
    # Add value labels on bars
    for bar in bars:
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.2f}', ha='center', va='bottom', fontweight='bold')
    
    # 2. Speedup Distribution (Histogram)
    ax2 = axes[0, 1]
    ax2.hist(merged_data['speedup'], bins=20, alpha=0.7, color='lightgreen', edgecolor='darkgreen')
    ax2.axvline(overall_stats['avg_speedup'], color='red', linestyle='--', 
                label=f'Mean: {overall_stats["avg_speedup"]:.2f}')
    ax2.axvline(overall_stats['median_speedup'], color='blue', linestyle='--', 
                label=f'Median: {overall_stats["median_speedup"]:.2f}')
    ax2.set_title('Distribution of Speedup Values', fontweight='bold')
    ax2.set_xlabel('Speedup Factor')
    ax2.set_ylabel('Frequency')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # 3. Execution Time Comparison (Scatter Plot)
    ax3 = axes[1, 0]
    # 1) equal aspect
    ax3.set_aspect('equal', adjustable='box')

    # 2) two‑color scatter
    winner = np.where(
        merged_data['total_time'] > merged_data['immv_time'],
        'IMMV faster', 'MV faster'
    )
    palette = {'IMMV faster':'C0', 'MV faster':'C1'}
    for w in ['IMMV faster','MV faster']:
        sel = winner==w
        ax3.scatter(
            merged_data.loc[sel,'immv_time'],
            merged_data.loc[sel,'total_time'],
            label=w, 
            color=palette[w],
            alpha=0.7, s=60
        )

    # 3) shaded regions
    xmin, xmax = ax3.get_xlim()
    ax3.fill_between(
        [xmin,xmax], [xmin,xmax], [ax3.get_ylim()[1]]*2,
        color='C0', alpha=0.1
    )
    ax3.fill_between(
        [xmin,xmax], [ax3.get_ylim()[0]]*2, [xmin,xmax],
        color='C1', alpha=0.1
    )

    # 45° line
    ax3.plot([xmin, xmax], [xmin, xmax], 'r--', alpha=0.7, label='Equal performance')

    ax3.set_title('Execution Time Comparison', fontweight='bold')
    ax3.set_xlabel('IMMV Time (ms)')
    ax3.set_ylabel('MV Total Time (ms)')
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # 4. Average Times by Write Index (Grouped Bar Chart)
    ax4 = axes[1, 1]
    x = np.arange(len(speedup_by_write_index))
    width = 0.35
    
    bars1 = ax4.bar(x - width/2, speedup_by_write_index['avg_mv_time'], 
                    width, label='MV (Write + Refresh)', alpha=0.7, color='coral')
    bars2 = ax4.bar(x + width/2, speedup_by_write_index['avg_immv_time'], 
                    width, label='IMMV (Write Only)', alpha=0.7, color='lightblue')
    
    ax4.set_title('Average Execution Times by Write Index', fontweight='bold')
    ax4.set_xlabel('Write Index')
    ax4.set_ylabel('Execution Time (milliseconds)')
    ax4.set_xticks(x)
    ax4.set_xticklabels(speedup_by_write_index['write_index'])
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    
    # Add value labels on bars
    for bars in [bars1, bars2]:
        for bar in bars:
            height = bar.get_height()
            ax4.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.2f}', ha='center', va='bottom', fontsize=8)
    
    plt.tight_layout()
    
    # Save the plot
    output_file = '/app/data/results/immv_vs_mv_speedup_analysis.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Visualization saved as '{output_file}'")
    
    plt.show()

def print_summary_statistics(merged_data, speedup_by_write_index, overall_stats):
    """Print comprehensive summary statistics."""
    
    print("\n" + "="*60)
    print("IMMV vs MATERIALIZED VIEW PERFORMANCE ANALYSIS")
    print("="*60)
    
    print(f"\nOVERALL STATISTICS:")
    print(f"Total Comparisons: {overall_stats['total_comparisons']}")
    print(f"Average Speedup: {overall_stats['avg_speedup']:.3f}x")
    print(f"Median Speedup: {overall_stats['median_speedup']:.3f}x")
    print(f"Standard Deviation: {overall_stats['std_speedup']:.3f}")
    print(f"Min Speedup: {overall_stats['min_speedup']:.3f}x")
    print(f"Max Speedup: {overall_stats['max_speedup']:.3f}x")
    
    print(f"\nSPEEDUP BY WRITE INDEX:")
    print("-" * 80)
    print(f"{'Write Index':<12} {'Avg Speedup':<12} {'Std Dev':<10} {'Count':<8} {'Avg MV Time':<12} {'Avg IMMV Time':<12}")
    print("-" * 80)
    
    for _, row in speedup_by_write_index.iterrows():
        print(f"{row['write_index']:<12} {row['avg_speedup']:<12.3f} {row['std_speedup']:<10.3f} "
              f"{row['count']:<8} {row['avg_mv_time']:<12.3f} {row['avg_immv_time']:<12.3f}")
    
    # Performance insights
    print(f"\nPERFORMANCE INSIGHTS:")
    print("-" * 40)
    
    if overall_stats['avg_speedup'] > 1:
        print(f"✓ IMMV shows better performance with {overall_stats['avg_speedup']:.2f}x speedup on average")
    else:
        print(f"✗ MV shows better performance with {1/overall_stats['avg_speedup']:.2f}x speedup on average")
    
    best_write_index = speedup_by_write_index.loc[speedup_by_write_index['avg_speedup'].idxmax()]
    worst_write_index = speedup_by_write_index.loc[speedup_by_write_index['avg_speedup'].idxmin()]
    
    print(f"• Best IMMV performance: Write Index {best_write_index['write_index']} "
          f"({best_write_index['avg_speedup']:.2f}x speedup)")
    print(f"• Worst IMMV performance: Write Index {worst_write_index['write_index']} "
          f"({worst_write_index['avg_speedup']:.2f}x speedup)")

def main():
    """Main function to run the analysis."""
    
    # Configuration
    csv_file = '/app/data/benchmark_results.csv'
    
    print("IMMV vs MV Performance Analysis")
    print("="*50)
    
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
    
    # Print summary statistics
    print_summary_statistics(merged_data, speedup_by_write_index, overall_stats)
    
    # Create visualizations
    print("\nCreating visualizations...")
    create_visualizations(merged_data, speedup_by_write_index, overall_stats)
    
    # Save detailed results to CSV
    output_csv = '/app/data/results/speedup_analysis_results.csv'
    speedup_by_write_index.to_csv(output_csv, index=False)
    print(f"\nDetailed results saved to '{output_csv}'")
    
    print("\nAnalysis complete!")

if __name__ == "__main__":
    main()