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
    
    # First ensure dataset exists
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_id} exists.")
    except Exception:
        dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
        dataset = client.create_dataset(dataset, exists_ok=True)
        print(f"Created dataset {dataset_id}")
    
    # Then create table if it doesn't exist
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
        print("Warning: Empty batch received, skipping upload")
        return 0
    
    try:
        print(f"\nPreparing to insert {len(rows)} rows into {table_id}...")
        
        # Convert to DataFrame and handle data types
        df = pd.DataFrame(rows)
        
        # Validate required columns
        required_columns = ['symbol', 'date', 'collected_date']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Convert numeric columns to appropriate types
        float_cols = ['strike', 'last', 'mark', 'bid', 'ask', 'implied_volatility', 'delta', 'gamma', 'theta', 'vega', 'rho']
        int_cols = ['bid_size', 'ask_size', 'volume', 'open_interest']
        
        for col in float_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        for col in int_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        
        # Convert date columns to YYYY-MM-DD format
        date_columns = ['expiration', 'date', 'collected_date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d')
        
        # Remove rows with missing critical data
        critical_cols = ['symbol', 'date', 'strike', 'type']
        initial_rows = len(df)
        df = df.dropna(subset=critical_cols)
        dropped_rows = initial_rows - len(df)
        if dropped_rows > 0:
            print(f"Warning: Dropped {dropped_rows} rows with missing critical data")
        
        if len(df) == 0:
            print("Warning: No valid rows to insert after data cleaning")
            return 0
        
        client = bigquery.Client(project=project_id)
        dataset_id, table_name = table_id.split('.')
        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_name)
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

def fetch_historical_options(symbol, date_range, table_id, project_id):
    """
    Fetch historical options data from Alpha Vantage API and store in BigQuery.
    
    This function implements Pro API rate limiting:
    - Maximum 70 calls per minute
    - 0.86 seconds between calls (60/70 ≈ 0.86)
    - Automatic pause when limit is reached
    """
    print("Fetching historical options with Pro API rate limit (70 calls/minute)...")
    create_options_table_if_not_exists(table_id, project_id)
    alpha_vantage_key = get_secret("alpha_vantage_api_key")
    calls_in_last_minute = 0
    last_call_time = time.time()
    current_batch = []
    total_rows_inserted = 0
    session = create_session_with_retries()
    current_month = None

    for idx, date in enumerate(date_range):
        current_date = pd.to_datetime(date)
        
        # Check if we're starting a new month
        date_month = current_date.strftime('%Y-%m')
        if current_month is None:
            current_month = date_month
        elif date_month != current_month:
            # Push the current month's batch before starting a new month
            if current_batch:
                print(f"\nCompleted month {current_month}, pushing {len(current_batch)} records to BigQuery...")
                rows_inserted = push_batch_to_bq(current_batch, table_id, project_id)
                total_rows_inserted += rows_inserted
                print(f"Inserted {rows_inserted} rows for {current_month}. Total rows so far: {total_rows_inserted}")
                current_batch = []  # Reset batch for new month
            current_month = date_month
        
        url = f"https://www.alphavantage.co/query?function=HISTORICAL_OPTIONS&symbol={symbol}&date={date}&apikey={alpha_vantage_key}"
        print(f"Fetching data for {symbol} on {date}...")
        
        try:
            response = session.get(url)
            
            if response.status_code == 503:
                print(f"Service unavailable for {symbol} on {date}. Will retry automatically...")
            elif response.status_code != 200:
                print(f"Error: Unable to fetch data for {symbol} on {date}. Status code: {response.status_code}")
                print("API Response:", response.text)
                continue
            
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
        except requests.exceptions.RequestException as e:
            print(f"Request failed after all retries for {symbol} on {date}: {e}")
            continue

        calls_in_last_minute += 1
        
        # Rate limiting logic
        current_time = time.time()
        if current_time - last_call_time >= 60:
            print(f"Resetting rate limit counter. Made {calls_in_last_minute} calls in the last minute.")
            calls_in_last_minute = 0
            last_call_time = current_time
        
        if calls_in_last_minute >= 70:  # Pro API limit
            wait_time = 60 - (current_time - last_call_time)
            if wait_time > 0:
                print(f"Reached rate limit (70 calls/minute). Waiting {wait_time:.2f} seconds...")
                time.sleep(wait_time)
                calls_in_last_minute = 0
                last_call_time = time.time()
        elif idx < len(date_range) - 1:
            time.sleep(0.86)  # Enforcing the delay explicitly

    # Push the final month's batch
    if current_batch:
        print(f"\nPushing final batch for {current_month} to BigQuery...")
        rows_inserted = push_batch_to_bq(current_batch, table_id, project_id)
        total_rows_inserted += rows_inserted
        print(f"Inserted {rows_inserted} rows for final month. Total rows: {total_rows_inserted}")
    
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
    # Generate date range and exclude weekends
    date_range = pd.date_range(start=date_start, end=date_end)
    # Monday = 0, Sunday = 6
    trading_days = date_range[date_range.weekday < 5].strftime("%Y-%m-%d").tolist()
    
    print(f"\n{'='*80}")
    print(f"Processing {symbol} from {date_start} to {date_end}")
    print(f"Will fetch {len(trading_days)} trading days (excluding weekends)")
    print(f"{'='*80}")
    
    try:
        rows_inserted = fetch_historical_options(symbol, trading_days, table_id, project_id)
        if rows_inserted > 0:
            print(f"Successfully completed {symbol}: {rows_inserted} rows inserted")
        else:
            print(f"No data inserted for {symbol}")
        return rows_inserted
    except Exception as e:
        print(f"Error processing {symbol}: {str(e)}")
        import traceback
        print("Full error traceback:")
        traceback.print_exc()
        return 0
    finally:
        print(f"Finished processing {symbol}")
        print(f"{'='*80}")

def main():
    config = get_config()
    project_id = config["project_id"]
    date_start = config["date_start"]
    
    # Read S&P 500 constituents
    sp500_file = "org_files/S&P 500 Constituents.csv"
    try:
        df_sp500 = pd.read_csv(sp500_file)
        symbols = df_sp500['Symbol'].tolist()
        print(f"Successfully loaded {len(symbols)} symbols from {sp500_file}")
    except Exception as e:
        print(f"Error reading {sp500_file}: {str(e)}")
        return

    total_symbols = len(symbols)
    total_rows_inserted = 0
    successful_symbols = []
    failed_symbols = []
    empty_symbols = []
    
    print(f"\nStarting data collection for {total_symbols} symbols")
    print(f"Start date: {date_start}")
    print(f"Project ID: {project_id}")
    print(f"{'='*80}")
    
    for idx, symbol in enumerate(symbols, 1):
        print(f"\nProcessing symbol {idx}/{total_symbols}: {symbol}")
        try:
            rows = process_symbol(symbol, project_id, date_start)
            
            if rows > 0:
                total_rows_inserted += rows
                successful_symbols.append(symbol)
                print(f"Progress: {idx}/{total_symbols} symbols processed")
                print(f"Current total rows: {total_rows_inserted}")
            else:
                empty_symbols.append(symbol)
                print(f"No data found for {symbol}")
        except KeyboardInterrupt:
            print("\nProcess interrupted by user!")
            break
        except Exception as e:
            print(f"Error processing {symbol}: {str(e)}")
            failed_symbols.append(symbol)
            continue
    
    # Print summary
    print(f"\n{'='*80}")
    print("Data Collection Summary")
    print(f"{'='*80}")
    print(f"Total symbols attempted: {total_symbols}")
    print(f"Successful symbols: {len(successful_symbols)}")
    print(f"Empty symbols: {len(empty_symbols)}")
    print(f"Failed symbols: {len(failed_symbols)}")
    print(f"Total rows inserted: {total_rows_inserted}")
    
    if successful_symbols:
        avg_rows = total_rows_inserted/len(successful_symbols)
        print(f"Average rows per successful symbol: {avg_rows:.2f}")
    
    # Print detailed symbol lists
    if empty_symbols:
        print(f"\nSymbols with no data ({len(empty_symbols)}):")
        for symbol in empty_symbols:
            print(f"- {symbol}")
    
    if failed_symbols:
        print(f"\nFailed symbols ({len(failed_symbols)}):")
        for symbol in failed_symbols:
            print(f"- {symbol}")
    
    if successful_symbols:
        print(f"\nSuccessful symbols ({len(successful_symbols)}):")
        for symbol in successful_symbols:
            print(f"- {symbol}")
    
    # Save summary to file
    summary_file = "data_collection_summary.txt"
    try:
        with open(summary_file, "w") as f:
            f.write("Data Collection Summary\n")
            f.write(f"Date: {pd.Timestamp.now()}\n")
            f.write(f"Start date: {date_start}\n")
            f.write(f"Total symbols: {total_symbols}\n")
            f.write(f"Total rows: {total_rows_inserted}\n")
            f.write(f"Successful symbols: {', '.join(successful_symbols)}\n")
            f.write(f"Empty symbols: {', '.join(empty_symbols)}\n")
            f.write(f"Failed symbols: {', '.join(failed_symbols)}\n")
        print(f"\nSummary saved to {summary_file}")
    except Exception as e:
        print(f"Failed to save summary file: {e}")

# Only run main if executed as a script, not on import
if __name__ == "__main__":
    main()



