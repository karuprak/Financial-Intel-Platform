from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import boto3
import json
import os
import io
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

COMPANIES = {
    'AAPL': '0000320193',
    'MSFT': '0000789019',
    'GOOGL': '0001652044',
    'AMZN': '0001018724',
    'TSLA': '0001318605',
    'META': '0001326801',
    'NVDA': '0001045810',
    'JPM': '0000019617',
    'WMT': '0000104169',
    'JNJ': '0000200406',
    'GS': '0000886982',
    'F': '0000037996',
    'GM': '0001467858',
    'PFE': '0000078003',
    'TGT': '0000027419',
    'NFLX': '0001065280',
    'BABA': '0001577552',
    'DIS': '0001001039',
    'UBER': '0001543151',
    'PYPL': '0001633917',
}

REVENUE_FIELDS = {
    'GS': 'RevenuesNetOfInterestExpense',
    'AMZN': 'RevenueFromContractWithCustomerExcludingAssessedTax',
    'JNJ': 'Revenues',
    'BABA': 'Revenues',
    'DEFAULT': 'Revenues'
}

default_args = {
    'owner': 'prakash',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

def download_sec_data():
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION')
    )

    bucket = os.getenv('S3_BUCKET_NAME')

    for ticker, cik in COMPANIES.items():
        print(f"Downloading data for {ticker}...")

        url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
        headers = {'User-Agent': 'prakash karuprak@gmail.com'}

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()

            s3.put_object(
                Bucket=bucket,
                Key=f"bronze/sec_filings/{ticker}/{datetime.now().strftime('%Y-%m-%d')}.json",
                Body=json.dumps(data),
                ContentType='application/json'
            )
            print(f"{ticker} saved to S3 Bronze")
        else:
            print(f"Failed to download {ticker}: {response.status_code}")


def transform_to_silver():
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION')
    )

    bucket = os.getenv('S3_BUCKET_NAME')
    today = datetime.now().strftime('%Y-%m-%d')

    for ticker in COMPANIES.keys():
        try:
            obj = s3.get_object(
                Bucket=bucket,
                Key=f"bronze/sec_filings/{ticker}/{today}.json"
            )
            data = json.loads(obj['Body'].read())

            revenue_field = REVENUE_FIELDS.get(ticker, REVENUE_FIELDS['DEFAULT'])
            filing_types = ['10-K', '10-Q', '20-F', '6-K']

            revenue_entries = data['facts']['us-gaap'][revenue_field]['units']['USD']

            rows = []
            for entry in revenue_entries:
                if entry.get('form') in filing_types and entry.get('val', 0) > 0:
                    rows.append({
                        'company_name': data.get('entityName', ticker),
                        'cik_number': data.get('cik', ''),
                        'period_end': entry.get('end', ''),
                        'revenue': entry.get('val', 0),
                        'filing_type': entry.get('form', '')
                    })

            if not rows:
                print(f"No data for {ticker}")
                continue

            seen = set()
            unique_rows = []
            for row in rows:
                key = (row['company_name'], row['period_end'], row['filing_type'])
                if key not in seen:
                    seen.add(key)
                    unique_rows.append(row)

            df = pd.DataFrame(unique_rows)
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)

            s3.put_object(
                Bucket=bucket,
                Key=f"silver/sec_filings/{ticker}/data.csv",
                Body=csv_buffer.getvalue()
            )
            print(f"{ticker} written to Silver")

        except Exception as e:
            print(f"Skipping {ticker}: {str(e)[:80]}")


def transform_to_gold():
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION')
    )

    bucket = os.getenv('S3_BUCKET_NAME')
    all_data = []

    for ticker in COMPANIES.keys():
        try:
            obj = s3.get_object(
                Bucket=bucket,
                Key=f"silver/sec_filings/{ticker}/data.csv"
            )
            df = pd.read_csv(io.StringIO(obj['Body'].read().decode('utf-8')))
            df['ticker'] = ticker
            all_data.append(df)
            print(f"Read {ticker} from Silver")

        except Exception as e:
            print(f"Skipping {ticker}: {str(e)[:80]}")

    df_all = pd.concat(all_data, ignore_index=True)

    df_all['revenue'] = pd.to_numeric(df_all['revenue'], errors='coerce')
    df_all['period_end'] = pd.to_datetime(df_all['period_end'], errors='coerce')
    df_all['year'] = df_all['period_end'].dt.year

    df_gold = df_all.groupby(['company_name', 'ticker', 'year', 'filing_type']).agg(
        total_revenue=('revenue', 'sum'),
        avg_revenue=('revenue', 'mean'),
        max_revenue=('revenue', 'max'),
        min_revenue=('revenue', 'min'),
        filing_count=('revenue', 'count')
    ).reset_index()

    df_gold['total_revenue'] = df_gold['total_revenue'].round(2)
    df_gold['avg_revenue'] = df_gold['avg_revenue'].round(2)
    df_gold['max_revenue'] = df_gold['max_revenue'].round(2)
    df_gold['min_revenue'] = df_gold['min_revenue'].round(2)

    csv_buffer = io.StringIO()
    df_gold.to_csv(csv_buffer, index=False)

    s3.put_object(
        Bucket=bucket,
        Key="gold/sec_filings/revenue_metrics.csv",
        Body=csv_buffer.getvalue()
    )
    print(f"Gold layer written to S3 successfully — {len(df_gold)} rows")

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    local_dashboard_path = os.path.join(project_root, 'dashboard', 'revenue_metrics.csv')
    df_gold.to_csv(local_dashboard_path, index=False)
    print(f"Gold layer written to local dashboard folder — {local_dashboard_path}")


with DAG(
    dag_id='sec_filing_pipeline',
    default_args=default_args,
    description='Downloads SEC filings and transforms Bronze to Silver to Gold automatically',
    schedule_interval='0 0 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sec', 'bronze', 'silver', 'gold', 'batch'],
) as dag:

    download_task = PythonOperator(
        task_id='download_sec_filings',
        python_callable=download_sec_data,
    )

    silver_task = PythonOperator(
        task_id='transform_to_silver',
        python_callable=transform_to_silver,
    )

    gold_task = PythonOperator(
        task_id='transform_to_gold',
        python_callable=transform_to_gold,
    )

    download_task >> silver_task >> gold_task