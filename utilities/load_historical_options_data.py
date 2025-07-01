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

def push_batch_to_bq(rows, table_id, project_id):
    """Helper function to push a batch of data to BigQuery"""
    if not rows:
        return 0
    
    try:
        print(f"\nPreparing to insert {len(rows)} rows into {table_id}...")
        df = pd.DataFrame(rows)
        
        # Convert date columns to YYYY-MM-DD format
        date_columns = ['expiration', 'date', 'collected_date']
        for col in date_columns:
            df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d')
        
        print("\nSample of data to be inserted:")
        print(df.head(1).to_string())
        
        client = bigquery.Client(project=project_id)
        table_ref = client.dataset(table_id.split('.')[0]).table(table_id.split('.')[1])
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
        
        job = client.load_table_from_json(records, table_ref, job_config=job_config)
        result = job.result()
        
        destination_table = client.get_table(table_ref)
        print(f"\nBatch upload completed. Current table size: {destination_table.num_rows} rows")
        return len(records)
        
    except Exception as e:
        print(f"\nFailed to insert batch into {table_id}")
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        import traceback
        print("\nFull error traceback:")
        traceback.print_exc()
        return 0

def fetch_historical_options(symbol, date_range, table_id, project_id, sleep_seconds=0.86):  # 0.86s = ~70 calls per minute
    print("Fetching historical options with Pro API rate limit (70 calls/minute)...")
    create_options_table_if_not_exists(table_id, project_id)
    alpha_vantage_key = get_secret("alpha_vantage_api_key")
    calls_in_last_minute = 0
    last_call_time = time.time()
    current_batch = []
    current_month = None
    total_rows_inserted = 0
    session = create_session_with_retries()

    for idx, date in enumerate(date_range):
        current_date = pd.to_datetime(date)
        # If we're starting a new month or this is the first date
        if current_month is None:
            current_month = current_date.strftime('%Y-%m')
        elif current_date.strftime('%Y-%m') != current_month:
            # Push the current batch before starting a new month
            print(f"\nCompleted month {current_month}, pushing batch to BigQuery...")
            rows_inserted = push_batch_to_bq(current_batch, table_id, project_id)
            total_rows_inserted += rows_inserted
            print(f"Inserted {rows_inserted} rows for {current_month}. Total rows so far: {total_rows_inserted}")
            # Reset for new month
            current_batch = []
            current_month = current_date.strftime('%Y-%m')
        
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
                        current_batch.append({
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

    # Push the final batch
    if current_batch:
        print(f"\nPushing final batch for {current_month} to BigQuery...")
        rows_inserted = push_batch_to_bq(current_batch, table_id, project_id)
        total_rows_inserted += rows_inserted
        print(f"Inserted {rows_inserted} rows for {current_month}. Final total: {total_rows_inserted}")
    
    if total_rows_inserted > 0:
        print(f"\nCompleted all data collection and uploads. Total rows inserted: {total_rows_inserted}")
        return total_rows_inserted
    else:
        print("No data was collected or inserted.")
        return 0

def process_symbol(symbol, project_id, date_start, date_end=None):
    """Process a single symbol and upload its data to BigQuery"""
    if date_end is None:
        date_end = pd.Timestamp.today().strftime("%Y-%m-%d")
    
    table_id = f"historical_data.{symbol.lower()}"
    date_range = pd.date_range(start=date_start, end=date_end).strftime("%Y-%m-%d").tolist()
    
    print(f"\n{'='*80}")
    print(f"Processing {symbol} from {date_start} to {date_end}")
    print(f"{'='*80}")
    
    try:
        rows_inserted = fetch_historical_options(symbol, date_range, table_id, project_id)
        print(f"Completed {symbol}: {rows_inserted} rows inserted")
        return rows_inserted
    except Exception as e:
        print(f"Error processing {symbol}: {str(e)}")
        return 0

def main():
    config = get_config()
    project_id = config["project_id"]
    date_start = config["date_start"]
    
    # Read S&P 500 constituents
    sp500_file = "org_files/S&P 500 Constituents.csv"
    try:
        df_sp500 = pd.read_csv(sp500_file)
        symbols = df_sp500['Symbol'].tolist()
    except Exception as e:
        print(f"Error reading {sp500_file}: {str(e)}")
        return
    
    total_symbols = len(symbols)
    total_rows_inserted = 0
    failed_symbols = []
    
    print(f"\nStarting data collection for {total_symbols} symbols")
    print(f"Start date: {date_start}")
    print(f"Project ID: {project_id}")
    
    for idx, symbol in enumerate(symbols, 1):
        print(f"\nProcessing symbol {idx}/{total_symbols}: {symbol}")
        rows = process_symbol(symbol, project_id, date_start)
        
        if rows > 0:
            total_rows_inserted += rows
        else:
            failed_symbols.append(symbol)
    
    print(f"\n{'='*80}")
    print("Data Collection Summary")
    print(f"{'='*80}")
    print(f"Total symbols processed: {total_symbols}")
    print(f"Total rows inserted: {total_rows_inserted}")
    print(f"Average rows per symbol: {total_rows_inserted/total_symbols:.2f}")
    
    if failed_symbols:
        print(f"\nFailed symbols ({len(failed_symbols)}):")
        for symbol in failed_symbols:
            print(f"- {symbol}")
    else:
        print("\nAll symbols processed successfully!")

# Only run main if executed as a script, not on import
if __name__ == "__main__":
    main()



