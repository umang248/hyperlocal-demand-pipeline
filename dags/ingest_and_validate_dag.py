import os
import sys
import json
from datetime import datetime, timedelta
import pandas as pd

# Add the scripts directory to path for imports
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), "scripts"))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
from db_utils import get_db_connection, get_db_engine

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def check_new_data_fn(**context):
    """Checks if there is new data in raw_orders since the last run."""
    print("Checking for new raw orders...")
    
    # Get last processed timestamp watermark
    last_processed_str = Variable.get("last_processed_timestamp", default_var="2026-01-01 00:00:00")
    last_processed = datetime.strptime(last_processed_str, "%Y-%m-%d %H:%M:%S")
    print(f"Last processed watermark: {last_processed}")
    
    conn = get_db_connection()
    try:
        query = "SELECT * FROM raw_orders WHERE timestamp > %s ORDER BY timestamp ASC LIMIT 500000"
        df = pd.read_sql(query, conn, params=(last_processed,))
    finally:
        conn.close()
        
    if df.empty:
        print("No new data found. Skipping subsequent tasks.")
        # Log empty run in pipeline_runs
        engine = get_db_engine()
        with engine.connect() as db_conn:
            db_conn.execute(
                "INSERT INTO pipeline_runs (dag_name, status, rows_processed) VALUES ('ingest_and_validate', 'SUCCESS', 0)"
            )
        raise AirflowSkipException("No new data to process.")
        
    print(f"Found {len(df)} new records.")
    
    # Convert timestamps to string for JSON serialization in XCom
    df['timestamp'] = df['timestamp'].dt.strftime("%Y-%m-%d %H:%M:%S")
    records = df.to_dict(orient="records")
    
    max_timestamp = df['timestamp'].max()
    
    # Push data to XCom
    context['ti'].xcom_push(key='new_records', value=records)
    context['ti'].xcom_push(key='max_timestamp', value=max_timestamp)
    
    return f"Found {len(records)} records. Max timestamp: {max_timestamp}"

def validate_schema_fn(**context):
    """Checks for nulls and correct datatypes."""
    records = context['ti'].xcom_pull(task_ids='check_new_data', key='new_records')
    if not records:
        raise AirflowSkipException("No records to validate.")
        
    invalid_ids = {}
    
    for idx, row in enumerate(records):
        reasons = []
        # Check required fields
        for col in ['order_id', 'zone_id', 'timestamp', 'order_value', 'items_count', 'delivery_time_mins']:
            if row.get(col) is None or str(row.get(col)).strip() == "":
                reasons.append(f"Missing required field: {col}")
                
        # Check data types
        try:
            float(row.get('order_value', 0))
        except (ValueError, TypeError):
            reasons.append("order_value is not numeric")
            
        try:
            int(row.get('items_count', 0))
        except (ValueError, TypeError):
            reasons.append("items_count is not integer")
            
        try:
            float(row.get('delivery_time_mins', 0))
        except (ValueError, TypeError):
            reasons.append("delivery_time_mins is not numeric")
            
        if reasons:
            invalid_ids[row['order_id']] = "; ".join(reasons)
            
    print(f"Schema validation complete. Found {len(invalid_ids)} invalid records out of {len(records)}.")
    context['ti'].xcom_push(key='schema_invalid_records', value=invalid_ids)
    return f"Validated {len(records)} records. Invalid schema: {len(invalid_ids)}"

def validate_ranges_fn(**context):
    """Validates value ranges and valid category values."""
    records = context['ti'].xcom_pull(task_ids='check_new_data', key='new_records')
    if not records:
        raise AirflowSkipException("No records to validate.")
        
    invalid_ids = {}
    valid_zones = {'Z1', 'Z2', 'Z3', 'Z4', 'Z5'}
    
    for row in records:
        reasons = []
        
        # Range checks
        try:
            val = float(row.get('order_value', 0))
            if val <= 0:
                reasons.append(f"order_value is not positive: {val}")
        except (ValueError, TypeError):
            pass
            
        try:
            items = int(row.get('items_count', 0))
            if items < 1 or items > 20:
                reasons.append(f"items_count out of range [1, 20]: {items}")
        except (ValueError, TypeError):
            pass
            
        # Category checks
        zone = row.get('zone_id')
        if zone not in valid_zones:
            reasons.append(f"Invalid zone_id: {zone}")
            
        if reasons:
            invalid_ids[row['order_id']] = "; ".join(reasons)
            
    print(f"Range validation complete. Found {len(invalid_ids)} invalid records out of {len(records)}.")
    context['ti'].xcom_push(key='range_invalid_records', value=invalid_ids)
    return f"Validated {len(records)} records. Invalid ranges: {len(invalid_ids)}"

def write_clean_data_fn(**context):
    """Splits into clean and validation_errors, inserts them, and updates watermark."""
    records = context['ti'].xcom_pull(task_ids='check_new_data', key='new_records')
    max_timestamp = context['ti'].xcom_pull(task_ids='check_new_data', key='max_timestamp')
    schema_invalid = context['ti'].xcom_pull(task_ids='validate_schema', key='schema_invalid_records') or {}
    range_invalid = context['ti'].xcom_pull(task_ids='validate_ranges', key='range_invalid_records') or {}
    
    if not records:
        raise AirflowSkipException("No records to write.")
        
    # Combine invalid reasons
    all_invalid = {}
    for oid, reason in schema_invalid.items():
        all_invalid[oid] = reason
    for oid, reason in range_invalid.items():
        if oid in all_invalid:
            all_invalid[oid] += " | " + reason
        else:
            all_invalid[oid] = reason
            
    clean_records = []
    error_records = []
    
    for row in records:
        oid = row['order_id']
        if oid in all_invalid:
            # Add reject reason
            err_row = row.copy()
            err_row['error_reason'] = all_invalid[oid]
            error_records.append(err_row)
        else:
            clean_records.append(row)
            
    # Write to database
    engine = get_db_engine()
    
    # Process clean records
    if clean_records:
        clean_df = pd.DataFrame(clean_records)
        clean_df['timestamp'] = pd.to_datetime(clean_df['timestamp'])
        clean_df['is_weekend'] = clean_df['is_weekend'].astype(bool)
        clean_df['is_festival_day'] = clean_df['is_festival_day'].astype(bool)
        print(f"Writing {len(clean_df)} clean records to clean_orders...")
        clean_df.to_sql("clean_orders", engine, if_exists="append", index=False)
        
    # Process error records
    if error_records:
        error_df = pd.DataFrame(error_records)
        error_df['timestamp'] = pd.to_datetime(error_df['timestamp'])
        error_df['is_weekend'] = error_df['is_weekend'].astype(bool)
        error_df['is_festival_day'] = error_df['is_festival_day'].astype(bool)
        print(f"Logging {len(error_df)} rejected records to validation_errors...")
        error_df.to_sql("validation_errors", engine, if_exists="append", index=False)
        
    # Update watermark Variable
    Variable.set("last_processed_timestamp", max_timestamp)
    print(f"Updated last_processed_timestamp watermark to: {max_timestamp}")
    
    # Log pipeline run
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO pipeline_runs (dag_name, status, rows_processed) VALUES (%s, %s, %s)",
                ('ingest_and_validate', 'SUCCESS', len(clean_records))
            )
        conn.commit()
    finally:
        conn.close()
        
    return f"Processed {len(records)} records. Clean: {len(clean_records)}, Errors: {len(error_records)}"

def on_failure_callback(context):
    """Callback for task failures to log status."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO pipeline_runs (dag_name, status, rows_processed) VALUES (%s, %s, %s)",
                ('ingest_and_validate', 'FAILED', 0)
            )
        conn.commit()
    finally:
        conn.close()

with DAG(
    'ingest_and_validate',
    default_args=default_args,
    description='Ingests new order records from raw_orders and validates them',
    schedule='0 * * * *',
    catchup=False,
    on_failure_callback=on_failure_callback
) as dag:

    check_new_data = PythonOperator(
        task_id='check_new_data',
        python_callable=check_new_data_fn,
    )

    validate_schema = PythonOperator(
        task_id='validate_schema',
        python_callable=validate_schema_fn,
    )

    validate_ranges = PythonOperator(
        task_id='validate_ranges',
        python_callable=validate_ranges_fn,
    )

    write_clean_data = PythonOperator(
        task_id='write_clean_data',
        python_callable=write_clean_data_fn,
    )

    # Dependencies
    check_new_data >> [validate_schema, validate_ranges] >> write_clean_data
