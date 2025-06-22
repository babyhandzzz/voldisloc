from cred_retrieval import get_secret
import pandas as pd
import requests
from push_to_bq import push_to_bq

def fetch_historical_options(symbol, date_range, table_id, project_id):
    """
    Fetch historical options data for a given symbol and date range from Alpha Vantage and push to BigQuery.
    Args:
        symbol (str): Stock ticker symbol.
        date_range (iterable): Iterable of date strings (YYYY-MM-DD).
        table_id (str): BigQuery table in the format 'dataset.table'.
        project_id (str): Google Cloud project ID.
    Returns:
        pd.DataFrame or None: The concatenated DataFrame if data exists, else None.
    """
    alpha_vantage_key = get_secret("alpha_vantage_api_key")
    all_data = []
    for date in date_range:
        url = f"https://www.alphavantage.co/query?function=HISTORICAL_OPTIONS&symbol={symbol}&date={date}&apikey={alpha_vantage_key}"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            #print(f"API Response for {symbol} on {date}:")
            #print(data)
            if 'data' in data:
                df = pd.json_normalize(data['data'])
                df['symbol'] = symbol
                df['date'] = date
                all_data.append(df)
            else:
                print(f"No data for {symbol} on {date}")
        else:
            print(f"Error: Unable to fetch data for {symbol} on {date}. Status code: {response.status_code}")
    if all_data:
        full_df = pd.concat(all_data, ignore_index=True)
        push_to_bq(full_df, table_id=table_id, project_id=project_id)
        print(f"Pushed {len(full_df)} rows to BigQuery table {table_id}.")
        return full_df
    else:
        print("No data returned.")
        return None

# Example usage:
if __name__ == "__main__":
    symbol = "AAPL"
    date_range = ["2025-06-20", "2025-06-21"]  # Example date range
    table_id = "historical_data.test"  # Replace with your dataset.table
    project_id = "voldilsloc"        # Replace with your GCP project ID
    fetch_historical_options(symbol, date_range, table_id, project_id)
