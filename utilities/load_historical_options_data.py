from utilities.cred_retrieval import get_secret
import requests
from google.cloud import bigquery
import time
import pandas as pd
import yaml
import random
from urllib3.util import Retry
from requests.adapters import HTTPAdapter

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

def create_session_with_retries():
    session = requests.Session()
    retry_strategy = Retry(
        total=5,  # maximum number of retries
        backoff_factor=1,  # wait 1, 2, 4, 8, 16 seconds between retries
        status_forcelist=[429, 500, 502, 503, 504],  # status codes to retry on
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def fetch_historical_options(symbol, date_range, table_id, project_id, sleep_seconds=0.86):  # 0.86s = ~70 calls per minute
    print("Fetching historical options with Pro API rate limit (70 calls/minute)...")
    create_options_table_if_not_exists(table_id, project_id)
    alpha_vantage_key = get_secret("alpha_vantage_api_key")
    calls_in_last_minute = 0
    last_call_time = time.time()
    all_rows = []
    session = create_session_with_retries()

    for idx, date in enumerate(date_range):
        url = f"https://www.alphavantage.co/query?function=HISTORICAL_OPTIONS&symbol={symbol}&date={date}&apikey={alpha_vantage_key}"
        print(f"Fetching data for {symbol} on {date}...")
        
        try:
            response = session.get(url)
            print(f"Response status code: {response.status_code}")
            
            if response.status_code == 503:
                print(f"Service unavailable for {symbol} on {date}. Will retry automatically...")
            
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and data['data']:
                    record_count = len(data['data'])
                    print(f"Found {record_count} records for {symbol} on {date}")
                    if record_count == 0:
                        print("Warning: Empty data array returned from API")
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
                print("API Response:", response.text)
                
        except requests.exceptions.RequestException as e:
            print(f"Request failed after all retries for {symbol} on {date}: {e}")
            continue

        calls_in_last_minute += 1
        
        # Reset counter if a minute has passed
        current_time = time.time()
        if current_time - last_call_time >= 60:
            print(f"Resetting rate limit counter. Made {calls_in_last_minute} calls in the last minute.")
            calls_in_last_minute = 0
            last_call_time = current_time
        
        # If we're approaching the rate limit, wait until the minute is up
        if calls_in_last_minute >= 70:
            wait_time = 60 - (current_time - last_call_time)
            if wait_time > 0:
                print(f"Reached rate limit (70 calls/minute). Waiting {wait_time:.2f} seconds...")
                time.sleep(wait_time)
                calls_in_last_minute = 0
                last_call_time = time.time()
        # Small delay between calls to prevent overwhelming the API
        elif idx < len(date_range) - 1:
            time.sleep(sleep_seconds)
    if all_rows:
        print(f"Preparing to insert {len(all_rows)} rows into {table_id}...")
        try:
            # Convert to pandas DataFrame for easier data validation
            df = pd.DataFrame(all_rows)
            print("\nData types before conversion:")
            print(df.dtypes)
            
            # Convert date columns to YYYY-MM-DD format
            date_columns = ['expiration', 'date', 'collected_date']
            for col in date_columns:
                df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d')
            
            print("\nSample of data to be inserted:")
            print(df.head(1).to_string())
            
            client = bigquery.Client(project=project_id)
            table_ref = client.dataset(table_id.split('.')[0]).table(table_id.split('.')[1])
            
            # Convert back to records
            records = df.to_dict('records')
            
            job_config = bigquery.LoadJobConfig(
                schema=[
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
                ],
                write_disposition="WRITE_APPEND"
            )
            
            # Start the load job
            job = client.load_table_from_json(records, table_ref, job_config=job_config)
            print("\nBigQuery job started. Waiting for completion...")
            
            # Wait for the job to complete and get the result
            result = job.result()  # This will raise an exception if the job fails
            
            # Get the destination table
            destination_table = client.get_table(table_ref)
            
            print(f"\nJob status: COMPLETED")
            print(f"Table size: {destination_table.num_rows} rows")
            print(f"Data uploaded successfully to {table_id}")
            
            if job.errors:
                print("\nErrors encountered:")
                for error in job.errors:
                    print(f"Error: {error['message']}")
                return 0
            
            print(f"\nSuccessfully inserted {len(records)} rows into {table_id}.")
            return len(records)
            
        except Exception as e:
            print(f"\nFailed to insert data into {table_id}")
            print(f"Error type: {type(e).__name__}")
            print(f"Error message: {str(e)}")
            import traceback
            print("\nFull error traceback:")
            traceback.print_exc()
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



