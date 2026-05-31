import boto3
import os
import io
import json
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

bucket = os.getenv("S3_BUCKET_NAME")

companies = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 
             'META', 'NVDA', 'JPM', 'WMT', 'JNJ',
             'GS', 'F', 'GM', 'PFE', 'TGT', 
             'NFLX', 'BABA', 'DIS', 'UBER', 'PYPL']

all_data = []

for company in companies:
    try:
        obj = s3.get_object(
            Bucket=bucket,
            Key=f"silver/sec_filings/{company}/data.csv"
        )
        df = pd.read_csv(io.StringIO(obj['Body'].read().decode('utf-8')))
        df['ticker'] = company
        all_data.append(df)
        print(f"Read {company} from Silver")

    except Exception as e:
        print(f"Skipping {company}: {str(e)[:80]}")

# Combine all companies into one big DataFrame
df_all = pd.concat(all_data, ignore_index=True)

# Clean the data
df_all['revenue'] = pd.to_numeric(df_all['revenue'], errors='coerce')
df_all['period_end'] = pd.to_datetime(df_all['period_end'], errors='coerce')
df_all['year'] = df_all['period_end'].dt.year

# Aggregate by company and year
df_gold = df_all.groupby(['company_name', 'ticker', 'year', 'filing_type']).agg(
    total_revenue=('revenue', 'sum'),
    avg_revenue=('revenue', 'mean'),
    max_revenue=('revenue', 'max'),
    min_revenue=('revenue', 'min'),
    filing_count=('revenue', 'count')
).reset_index()

# Round revenue to 2 decimal places
df_gold['total_revenue'] = df_gold['total_revenue'].round(2)
df_gold['avg_revenue'] = df_gold['avg_revenue'].round(2)
df_gold['max_revenue'] = df_gold['max_revenue'].round(2)
df_gold['min_revenue'] = df_gold['min_revenue'].round(2)

print(f"Gold data created — {len(df_gold)} rows")

# Save to S3 Gold
csv_buffer = io.StringIO()
df_gold.to_csv(csv_buffer, index=False)

s3.put_object(
    Bucket=bucket,
    Key="gold/sec_filings/revenue_metrics.csv",
    Body=csv_buffer.getvalue()
)

print("Gold layer written to S3 successfully!")
print(f"Total records: {len(df_gold)}")
print(f"Companies: {df_gold['ticker'].nunique()}")
print(f"Years covered: {df_gold['year'].min()} - {df_gold['year'].max()}")