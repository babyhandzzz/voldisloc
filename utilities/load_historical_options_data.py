from utilities.cred_retrieval import get_secret
import requests
from google.cloud import bigquery
import time
import pandas as pd
import yaml
import random  # Added for jitter

def get_config():
    print("Loading config.yaml...")
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)
    return config

def create_options_table_if_not_exists(table_id, project_id):
    client = bigquery.Client(project=project_id)
    dataset_id, table_name = table_id.split('.')
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_name)
    schema = [
        bigquery.SchemaField('contractID', 'STRING'),
        bigquery.SchemaField('symbol', 'STRING'),
        bigquery.SchemaField('expiration', 'DATE'),
        bigquery.SchemaField('strike', 'FLOAT'),
        bigquery.SchemaField('type', 'STRING'),
        bigquery.SchemaField('last', 'FLOAT'),
        bigquery.SchemaField('mark', 'FLOAT'),
        bigquery.SchemaField('bid', 'FLOAT'),
        bigquery.SchemaField('bid_size', 'INTEGER'),
        bigquery.SchemaField('ask', 'FLOAT'),
        bigquery.SchemaField('ask_size', 'INTEGER'),
        bigquery.SchemaField('volume', 'INTEGER'),
        bigquery.SchemaField('open_interest', 'INTEGER'),
        bigquery.SchemaField('date', 'DATE'),
        bigquery.SchemaField('implied_volatility', 'FLOAT'),
        bigquery.SchemaField('delta', 'FLOAT'),
        bigquery.SchemaField('gamma', 'FLOAT'),
        bigquery.SchemaField('theta', 'FLOAT'),
        bigquery.SchemaField('vega', 'FLOAT'),
        bigquery.SchemaField('rho', 'FLOAT'),
        bigquery.SchemaField('collected_date', 'DATE')
    ]
    table = bigquery.Table(table_ref, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="date"
    )
    table.clustering_fields = ["symbol", "expiration", "type"]
    try:
        client.get_table(table_ref)
        print(f"Table {table_id} already exists.")
    except Exception:
        client.create_table(table)
        print(f"Created table {table_id} with partitioning and clustering.")

def fetch_historical_options(symbol, date_range, table_id, project_id, sleep_seconds=17):
    print("Fetching historical options...")
    create_options_table_if_not_exists(table_id, project_id)
    alpha_vantage_key = get_secret("alpha_vantage_api_key")
    all_rows = []
    for idx, date in enumerate(date_range):
        url = f"https://www.alphavantage.co/query?function=HISTORICAL_OPTIONS&symbol={symbol}&date={date}&apikey={alpha_vantage_key}"
        print(f"Fetching data for {symbol} on {date}...")
        response = requests.get(url)
        print(f"Response status code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"API Response for {symbol} on {date}: {data}")
            if 'data' in data and data['data']:
                print(f"Found {len(data['data'])} records for {symbol} on {date}")
                for row in data['data']:
                    all_rows.append({
                        'contractID': row.get('contractID'),
                        'symbol': row.get('symbol', symbol),
                        'expiration': row.get('expiration'),
                        'strike': float(row.get('strike')) if row.get('strike') is not None else None,
                        'type': row.get('type'),
                        'last': float(row.get('last')) if row.get('last') is not None else None,
                        'mark': float(row.get('mark')) if row.get('mark') is not None else None,
                        'bid': float(row.get('bid')) if row.get('bid') is not None else None,
                        'bid_size': int(row.get('bid_size')) if row.get('bid_size') is not None else None,
                        'ask': float(row.get('ask')) if row.get('ask') is not None else None,
                        'ask_size': int(row.get('ask_size')) if row.get('ask_size') is not None else None,
                        'volume': int(row.get('volume')) if row.get('volume') is not None else None,
                        'open_interest': int(row.get('open_interest')) if row.get('open_interest') is not None else None,
                        'date': row.get('date', date),
                        'implied_volatility': float(row.get('implied_volatility')) if row.get('implied_volatility') is not None else None,
                        'delta': float(row.get('delta')) if row.get('delta') is not None else None,
                        'gamma': float(row.get('gamma')) if row.get('gamma') is not None else None,
                        'theta': float(row.get('theta')) if row.get('theta') is not None else None,
                        'vega': float(row.get('vega')) if row.get('vega') is not None else None,
                        'rho': float(row.get('rho')) if row.get('rho') is not None else None,
                        'collected_date': date
                    })
            else:
                print(f"No data for {symbol} on {date}")
        else:
            print(f"Error: Unable to fetch data for {symbol} on {date}. Status code: {response.status_code}")
            if response.status_code == 429:
                print("API rate limit hit. Response:", response.text)
                # Add additional sleep time if we hit rate limits
                time.sleep(60)  # Wait a full minute before continuing
            else:
                print("API Response:", response.text)
        if idx < len(date_range) - 1:
            jitter = random.uniform(-2, 2)
            sleep_time = sleep_seconds + jitter
            print(f"Sleeping {sleep_time:.2f} seconds to avoid API rate limits (base {sleep_seconds}s + jitter {jitter:.2f}s)...")
            time.sleep(sleep_time)
    if all_rows:
        print(f"Preparing to insert {len(all_rows)} rows into {table_id}...")
        try:
            client = bigquery.Client(project=project_id)
            schema = [
                bigquery.SchemaField('contractID', 'STRING'),
                bigquery.SchemaField('symbol', 'STRING'),
                bigquery.SchemaField('expiration', 'DATE'),
                bigquery.SchemaField('strike', 'FLOAT'),
                bigquery.SchemaField('type', 'STRING'),
                bigquery.SchemaField('last', 'FLOAT'),
                bigquery.SchemaField('mark', 'FLOAT'),
                bigquery.SchemaField('bid', 'FLOAT'),
                bigquery.SchemaField('bid_size', 'INTEGER'),
                bigquery.SchemaField('ask', 'FLOAT'),
                bigquery.SchemaField('ask_size', 'INTEGER'),
                bigquery.SchemaField('volume', 'INTEGER'),
                bigquery.SchemaField('open_interest', 'INTEGER'),
                bigquery.SchemaField('date', 'DATE'),
                bigquery.SchemaField('implied_volatility', 'FLOAT'),
                bigquery.SchemaField('delta', 'FLOAT'),
                bigquery.SchemaField('gamma', 'FLOAT'),
                bigquery.SchemaField('theta', 'FLOAT'),
                bigquery.SchemaField('vega', 'FLOAT'),
                bigquery.SchemaField('rho', 'FLOAT'),
                bigquery.SchemaField('collected_date', 'DATE')
            ]
            table_ref = client.dataset(table_id.split('.')[0]).table(table_id.split('.')[1])
            job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_APPEND")
            job = client.load_table_from_json(all_rows, table_ref, job_config=job_config)
            job.result()
            print(f"Successfully inserted {len(all_rows)} rows into {table_id}.")
            return len(all_rows)
        except Exception as e:
            print(f"Failed to insert data into {table_id}: {e}")
            return 0
    else:
        print("No data returned.")
        return 0

def main():
    config = get_config()
    project_id = config["project_id"]
    symbol = config["symbol"]
    table_id = config["table_id"]
    date_start = config["date_start"]
    today = pd.Timestamp.today().strftime("%Y-%m-%d")
    date_range = pd.date_range(start=date_start, end=today).strftime("%Y-%m-%d").tolist()
    fetch_historical_options(symbol, date_range, table_id, project_id)

# Only run main if executed as a script, not on import
if __name__ == "__main__":
    main()



