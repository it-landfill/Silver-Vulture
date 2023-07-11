import pandas as pd

# Load rating_complete.csv, gather 500 samnples and save them in rating_sample_500.csv
samples=10000000
df = pd.read_csv('rating_complete.csv')
df = df.sample(n=samples)
df.to_csv(f'rating_sample_{samples}.csv', index=False)
