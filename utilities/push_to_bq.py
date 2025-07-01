from google.cloud import bigquery

def push_to_bq(rows, table_id, project_id, write_disposition="WRITE_APPEND", table_schema=None):
    """
    Push data to BigQuery using the BigQuery client directly.

    Args:
        rows (list): List of dictionaries containing the data to upload.
        table_id (str): BigQuery table in the format 'dataset.table'.
        project_id (str): Google Cloud project ID.
        write_disposition (str): BigQuery write disposition. Default is 'WRITE_APPEND'.
        table_schema (list): BigQuery table schema. Required if table doesn't exist.
    """
    
    if not rows:
        print("Warning: Empty data received, skipping upload")
        return 0
    
    try:
        print(f"\nPreparing to insert {len(rows)} rows into {table_id}...")
        
        client = bigquery.Client(project=project_id)
        dataset_id, table_name = table_id.split('.')
        dataset_ref = client.dataset(dataset_id)
        
        # First ensure dataset exists
        try:
            client.get_dataset(dataset_ref)
            print(f"Dataset {dataset_id} exists.")
        except Exception:
            dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
            dataset = client.create_dataset(dataset, exists_ok=True)
            print(f"Created dataset {dataset_id}")
        
        table_ref = dataset_ref.table(table_name)
        
        job_config = bigquery.LoadJobConfig(
            schema=table_schema,
            write_disposition=write_disposition
        )
        
        print(f"Starting upload with write_disposition: {write_disposition}")
        job = client.load_table_from_json(rows, table_ref, job_config=job_config)
        result = job.result()  # Wait for the job to complete
        
        destination_table = client.get_table(table_ref)
        print(f"\nBatch upload completed. Current table size: {destination_table.num_rows} rows")
        return len(rows)
        
    except Exception as e:
        print(f"\nFailed to push data to BigQuery table {table_id}")
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        import traceback
        print("\nFull error traceback:")
        traceback.print_exc()
        return 0  # Return 0 to indicate no rows were inserted


