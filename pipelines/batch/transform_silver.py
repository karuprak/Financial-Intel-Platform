from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
import boto3
import os
import io
import json
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

spark = SparkSession.builder \
    .appName("SEC Filings Bronze to Silver") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

bucket = os.getenv("S3_BUCKET_NAME")

REVENUE_FIELDS = {
    'GS':   'RevenuesNetOfInterestExpense',
    'AMZN': 'RevenueFromContractWithCustomerExcludingAssessedTax',
    'JNJ':  'Revenues',
    'BABA': 'Revenues',
    'DEFAULT': 'Revenues'
}

companies = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 
             'META', 'NVDA', 'JPM', 'WMT', 'JNJ',
             'GS', 'F', 'GM', 'PFE', 'TGT', 
             'NFLX', 'BABA', 'DIS', 'UBER', 'PYPL']

for company in companies:
    try:
        obj = s3.get_object(
            Bucket=bucket,
            Key=f"bronze/sec_filings/{company}/2026-05-29.json"
        )
        data = json.loads(obj['Body'].read())

        revenue_field = REVENUE_FIELDS.get(company, REVENUE_FIELDS['DEFAULT'])
        revenue_entries = data['facts']['us-gaap'][revenue_field]['units']['USD']

        rows = []
        for entry in revenue_entries:
            if entry.get('form') in ['10-K', '10-Q', '20-F', '6-K'] and entry.get('val', 0) > 0:
                rows.append({
                    'company_name': data.get('entityName', company),
                    'cik_number': data.get('cik', ''),
                    'period_end': entry.get('end', ''),
                    'revenue': entry.get('val', 0),
                    'filing_type': entry.get('form', '')
                })

        if not rows:
            print(f"No data for {company}")
            continue

        seen = set()
        unique_rows = []
        for row in rows:
            key = (row['company_name'], row['period_end'], row['filing_type'])
            if key not in seen:
                seen.add(key)
                unique_rows.append(row)

        df_pandas = pd.DataFrame(unique_rows)
        csv_buffer = io.StringIO()
        df_pandas.to_csv(csv_buffer, index=False)

        s3.put_object(
            Bucket=bucket,
            Key=f"silver/sec_filings/{company}/data.csv",
            Body=csv_buffer.getvalue()
        )

        print(f"{company} written to Silver — {len(unique_rows)} rows")

    except Exception as e:
        print(f"Skipping {company}: {str(e)[:80]}")

print("Silver layer complete!")
spark.stop()