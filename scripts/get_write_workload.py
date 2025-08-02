import pandas as pd

df = pd.read_csv('/app/data/workload.csv')
write_workload = df[df['query_type'].isin(['insert', 'update', 'delete'])]
write_workload.to_csv('/app/data/write_workload.csv', index=False)