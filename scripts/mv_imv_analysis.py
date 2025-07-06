import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from collections import defaultdict
import warnings
warnings.filterwarnings('ignore')

def load_and_analyze_data(csv_file_path):
    """
    Load CSV data and analyze MV vs IMV performance speedup
    """
    # Read the CSV file
    df = pd.read_csv(csv_file_path)
    
    # Strip whitespace from column names
    df.columns = df.columns.str.strip()
    
    print("Data Overview:")
    print(f"Total rows: {len(df)}")
    print(f"Experiments: {df['experiment_id'].unique()}")
    print(f"Configurations: {df['configuration'].unique()}")
    print(f"Operation types: {df['operation_type'].unique()}")
    print("\n")
    
    return df

def calculate_mv_total_time(df):
    """
    Calculate total execution time for MV setup including refresh operations
    """
    mv_times = []
    
    # Group by experiment and write_index for MV operations
    mv_data = df[df['configuration'] == 'materialized_view'].copy()
    
    for exp_id in mv_data['experiment_id'].unique():
        exp_data = mv_data[mv_data['experiment_id'] == exp_id]
        
        # Get all write operations for this experiment
        write_operations = exp_data[exp_data['operation_type'] == 'write']
        
        for write_idx in write_operations['write_index'].unique():
            if pd.isna(write_idx):
                continue
                
            # Get write operation time
            write_op = write_operations[write_operations['write_index'] == write_idx]
            if len(write_op) == 0:
                continue
                
            write_time = write_op['plan_execution_time'].iloc[0]
            
            # Get corresponding refresh operation time
            refresh_op = exp_data[
                (exp_data['operation_type'] == 'refresh') & 
                (exp_data['write_index'] == write_idx)
            ]
            
            if len(refresh_op) > 0:
                refresh_time = refresh_op['execution_time'].iloc[0]
                total_time = write_time + refresh_time
            else:
                total_time = write_time
            
            mv_times.append({
                'experiment_id': exp_id,
                'write_index': write_idx,
                'mv_total_time': total_time,
                'write_time': write_time,
                'refresh_time': refresh_time if len(refresh_op) > 0 else 0
            })
    
    return pd.DataFrame(mv_times)

def calculate_speedup_analysis(df):
    """
    Calculate speedup between MV and IMV setups for each write operation
    """
    speedup_data = []
    
    # Get MV total times (including refresh)
    mv_times_df = calculate_mv_total_time(df)
    
    # Get IMV times
    imv_data = df[
        (df['configuration'] == 'incremental_view') & 
        (df['operation_type'] == 'write')
    ].copy()
    
    # Match MV and IMV times by experiment and write_index
    for _, mv_row in mv_times_df.iterrows():
        exp_id = mv_row['experiment_id']
        write_idx = mv_row['write_index']
        mv_total_time = mv_row['mv_total_time']
        
        # Find corresponding IMV time
        imv_match = imv_data[
            (imv_data['experiment_id'] == exp_id) & 
            (imv_data['write_index'] == write_idx)
        ]
        
        if len(imv_match) > 0:
            imv_time = imv_match['plan_execution_time'].iloc[0]
            
            # Calculate speedup (MV_time / IMV_time)
            # Values > 1 mean IMV is faster, < 1 mean MV is faster
            speedup = mv_total_time / imv_time if imv_time > 0 else np.nan
            
            speedup_data.append({
                'experiment_id': exp_id,
                'write_index': write_idx,
                'mv_total_time': mv_total_time,
                'imv_time': imv_time,
                'speedup': speedup,
                'mv_write_time': mv_row['write_time'],
                'mv_refresh_time': mv_row['refresh_time']
            })
    
    return pd.DataFrame(speedup_data)

def create_visualizations(speedup_df):
    """
    Create visualizations for the speedup analysis
    """
    # Set up the plotting style
    plt.style.use('default')
    sns.set_palette("husl")
    
    # Create a figure with multiple subplots
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('MV vs IMV Performance Analysis', fontsize=16, fontweight='bold')
    
    # 1. Speedup by Write Operation Index
    ax1 = axes[0, 0]
    speedup_by_write = speedup_df.groupby('write_index')['speedup'].agg(['mean', 'std']).reset_index()
    
    ax1.bar(speedup_by_write['write_index'], speedup_by_write['mean'], 
            yerr=speedup_by_write['std'], capsize=5, alpha=0.7)
    ax1.axhline(y=1, color='red', linestyle='--', alpha=0.7, label='Equal Performance')
    ax1.set_xlabel('Write Operation Index')
    ax1.set_ylabel('Average Speedup (MV/IMV)')
    ax1.set_title('Average Speedup by Write Operation')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # 2. Speedup Distribution
    ax2 = axes[0, 1]
    ax2.hist(speedup_df['speedup'].dropna(), bins=15, alpha=0.7, edgecolor='black')
    ax2.axvline(x=1, color='red', linestyle='--', alpha=0.7, label='Equal Performance')
    ax2.axvline(x=speedup_df['speedup'].mean(), color='green', linestyle='-', 
                alpha=0.7, label=f'Mean: {speedup_df["speedup"].mean():.2f}')
    ax2.set_xlabel('Speedup (MV/IMV)')
    ax2.set_ylabel('Frequency')
    ax2.set_title('Distribution of Speedup Values')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # 3. Execution Time Comparison
    ax3 = axes[1, 0]
    write_indices = speedup_df['write_index'].unique()
    x = np.arange(len(write_indices))
    width = 0.35
    
    mv_times = [speedup_df[speedup_df['write_index'] == wi]['mv_total_time'].mean() 
                for wi in write_indices]
    imv_times = [speedup_df[speedup_df['write_index'] == wi]['imv_time'].mean() 
                 for wi in write_indices]
    
    ax3.bar(x - width/2, mv_times, width, label='MV (Write + Refresh)', alpha=0.7)
    ax3.bar(x + width/2, imv_times, width, label='IMV (Write)', alpha=0.7)
    ax3.set_xlabel('Write Operation Index')
    ax3.set_ylabel('Average Execution Time (seconds)')
    ax3.set_title('Execution Time Comparison')
    ax3.set_xticks(x)
    ax3.set_xticklabels(write_indices)
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # 4. Speedup by Experiment
    ax4 = axes[1, 1]
    speedup_by_exp = speedup_df.groupby('experiment_id')['speedup'].agg(['mean', 'std']).reset_index()
    
    ax4.bar(speedup_by_exp['experiment_id'], speedup_by_exp['mean'], 
            yerr=speedup_by_exp['std'], capsize=5, alpha=0.7)
    ax4.axhline(y=1, color='red', linestyle='--', alpha=0.7, label='Equal Performance')
    ax4.set_xlabel('Experiment ID')
    ax4.set_ylabel('Average Speedup (MV/IMV)')
    ax4.set_title('Average Speedup by Experiment')
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.show()
    
    return fig

def print_summary_statistics(speedup_df):
    """
    Print summary statistics of the analysis
    """
    print("=" * 60)
    print("PERFORMANCE ANALYSIS SUMMARY")
    print("=" * 60)
    
    print(f"Total write operations analyzed: {len(speedup_df)}")
    print(f"Average speedup (MV/IMV): {speedup_df['speedup'].mean():.3f}")
    print(f"Median speedup (MV/IMV): {speedup_df['speedup'].median():.3f}")
    print(f"Standard deviation: {speedup_df['speedup'].std():.3f}")
    print(f"Min speedup: {speedup_df['speedup'].min():.3f}")
    print(f"Max speedup: {speedup_df['speedup'].max():.3f}")
    
    print("\nInterpretation:")
    avg_speedup = speedup_df['speedup'].mean()
    if avg_speedup > 1:
        print(f"• IMV is on average {avg_speedup:.2f}x faster than MV")
        print("• IMV shows better performance overall")
    elif avg_speedup < 1:
        print(f"• MV is on average {1/avg_speedup:.2f}x faster than IMV")
        print("• MV shows better performance overall")
    else:
        print("• Both approaches show similar performance")
    
    # Performance by write operation
    print("\nPerformance by Write Operation:")
    print("-" * 40)
    for write_idx in sorted(speedup_df['write_index'].unique()):
        subset = speedup_df[speedup_df['write_index'] == write_idx]
        avg_speedup = subset['speedup'].mean()
        print(f"Write {write_idx}: {avg_speedup:.3f}x speedup")
    
    # Performance by experiment
    print("\nPerformance by Experiment:")
    print("-" * 40)
    for exp_id in sorted(speedup_df['experiment_id'].unique()):
        subset = speedup_df[speedup_df['experiment_id'] == exp_id]
        avg_speedup = subset['speedup'].mean()
        print(f"{exp_id}: {avg_speedup:.3f}x speedup")
    
    print("\n" + "=" * 60)

def main():
    """
    Main function to run the analysis
    """
    # Load and analyze the data
    csv_file = '/app/data/benchmark_results.csv'
    
    print("Loading experiment data...")
    df = load_and_analyze_data(csv_file)
    
    print("Calculating speedup analysis...")
    speedup_df = calculate_speedup_analysis(df)
    
    if len(speedup_df) == 0:
        print("No matching data found for speedup analysis!")
        return
    
    print("Creating visualizations...")
    fig = create_visualizations(speedup_df)
    
    print("Generating summary statistics...")
    print_summary_statistics(speedup_df)
    
    # Save detailed results
    output_file = '/app/data/results/speedup_analysis_results.csv'
    speedup_df.to_csv(output_file, index=False)
    print(f"\nDetailed results saved to: {output_file}")
    
    # Save the plot
    save_path = '/app/data/results/mv_vs_imv_speedup_analysis.png'
    fig.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"Visualization saved to: {save_path}")

if __name__ == "__main__":
    main()