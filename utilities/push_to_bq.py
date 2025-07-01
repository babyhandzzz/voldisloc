import pandas as pd

def push_to_bq(df, table_id, project_id, if_exists='replace', table_schema=None):
    """
    Push a pandas DataFrame to Google BigQuery.

    Args:
        df (pd.DataFrame): DataFrame to upload.
        table_id (str): BigQuery table in the format 'dataset.table'.
        project_id (str): Google Cloud project ID.
        if_exists (str): 'fail', 'replace', or 'append'. Default is 'replace'.
        table_schema (list, optional): BigQuery table schema. Default is None.
    """
    
    try:
        df.to_gbq(destination_table=table_id,
                  project_id=project_id,
                  if_exists=if_exists,
                  table_schema=table_schema)
        print(f"Data pushed to BigQuery table {table_id} successfully.")
    except Exception as e:
        print(f"Failed to push data to BigQuery: {e}")


