import pandas as pd
import argparse

def main():
    parser = argparse.ArgumentParser(description='Extract write workload from CSV file')
    parser.add_argument('--input_csv', help='Path to the input CSV file')
    parser.add_argument('--output_csv', help='Path to the output CSV file')
    
    args = parser.parse_args()
    
    df = pd.read_csv(args.input_csv)
    write_workload = df[df['query_type'].isin(['insert', 'update', 'delete'])]
    write_workload.to_csv(args.output_csv, index=False)
    
    print(f"Write workload extracted from {args.input_csv} and saved to {args.output_csv}")

if __name__ == "__main__":
    main()