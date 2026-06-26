import os
import sys
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# Add the scripts directory to path for imports
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), "scripts"))

from airflow import DAG
from airflow.operators.python import PythonOperator
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

# Temporary files directory
TEMP_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "temp")
os.makedirs(TEMP_DIR, exist_ok=True)

def aggregate_hourly_fn(**context):
    """Groups clean_orders by zone + hour -> order_count."""
    print("Running aggregate_hourly...")
    engine = get_db_engine()
    
    orders_df = pd.read_sql("SELECT * FROM clean_orders", engine)
    if orders_df.empty:
        print("No clean orders found. Skipping.")
        raise AirflowSkipException("No data in clean_orders.")
        
    orders_df['timestamp'] = pd.to_datetime(orders_df['timestamp'])
    orders_df['hour_timestamp'] = orders_df['timestamp'].dt.floor('h')
    
    # Aggregate
    agg_df = orders_df.groupby(['zone_id', 'hour_timestamp']).agg(
        order_count=('order_id', 'count'),
        is_festival_day=('is_festival_day', 'max'),
        weather_condition=('weather_condition', lambda x: x.mode()[0] if not x.empty else 'sunny')
    ).reset_index()
    
    agg_df = agg_df.rename(columns={'hour_timestamp': 'timestamp'})
    
    # Reindex to include a complete hourly grid for all zones to avoid time-series gaps
    min_time = agg_df['timestamp'].min()
    max_time = agg_df['timestamp'].max()
    all_hours = pd.date_range(start=min_time, end=max_time, freq='h')
    all_zones = ["Z1", "Z2", "Z3", "Z4", "Z5"]
    
    grid = pd.MultiIndex.from_product([all_zones, all_hours], names=['zone_id', 'timestamp']).to_frame().reset_index(drop=True)
    df = pd.merge(grid, agg_df, on=['zone_id', 'timestamp'], how='left')
    
    # Fill missing values
    df['order_count'] = df['order_count'].fillna(0).astype(int)
    
    # Resolve metadata for empty hours
    hour_metadata = agg_df.groupby('timestamp').agg(
        hour_weather=('weather_condition', lambda x: x.mode()[0] if not x.empty else 'sunny'),
        hour_fest=('is_festival_day', 'max')
    ).reset_index()
    
    df = pd.merge(df, hour_metadata, on='timestamp', how='left')
    df['weather_condition'] = df['weather_condition'].fillna(df['hour_weather']).fillna('sunny')
    df['is_festival_day'] = df['is_festival_day'].fillna(df['hour_fest']).fillna(False).astype(bool)
    df = df.drop(columns=['hour_weather', 'hour_fest'])
    
    df = df.sort_values(by=['zone_id', 'timestamp']).reset_index(drop=True)
    
    # Save to temp CSV
    temp_file = os.path.join(TEMP_DIR, "aggregated.csv")
    df.to_csv(temp_file, index=False)
    print(f"Aggregated data written to {temp_file}")
    
    context['ti'].xcom_push(key='temp_file', value=temp_file)
    return temp_file

def engineer_time_features_fn(**context):
    """Adds all time-based features."""
    temp_file = context['ti'].xcom_pull(task_ids='aggregate_hourly', key='temp_file')
    if not temp_file or not os.path.exists(temp_file):
        raise FileNotFoundError("Aggregated temp file not found.")
        
    df = pd.read_csv(temp_file)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    df['hour_of_day'] = df['timestamp'].dt.hour
    df['day_of_week'] = df['timestamp'].dt.dayofweek
    df['is_weekend'] = (df['day_of_week'] >= 5).astype(bool)
    df['month'] = df['timestamp'].dt.month
    df['week_of_year'] = df['timestamp'].dt.isocalendar().week.astype(int)
    
    # Save to next temp CSV
    out_file = os.path.join(TEMP_DIR, "time_features.csv")
    df.to_csv(out_file, index=False)
    print(f"Time features written to {out_file}")
    
    context['ti'].xcom_push(key='temp_file', value=out_file)
    return out_file

def engineer_lag_features_fn(**context):
    """Computes lag and rolling features per zone."""
    temp_file = context['ti'].xcom_pull(task_ids='engineer_time_features', key='temp_file')
    if not temp_file or not os.path.exists(temp_file):
        raise FileNotFoundError("Time features temp file not found.")
        
    df = pd.read_csv(temp_file)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values(by=['zone_id', 'timestamp']).reset_index(drop=True)
    
    # Lag features
    print("Computing lags...")
    df['lag_1h'] = df.groupby('zone_id')['order_count'].shift(1)
    df['lag_2h'] = df.groupby('zone_id')['order_count'].shift(2)
    df['lag_24h'] = df.groupby('zone_id')['order_count'].shift(24)
    df['lag_168h'] = df.groupby('zone_id')['order_count'].shift(168)
    
    # Rolling features (using shifted target to avoid leakage)
    print("Computing rolling averages...")
    df['rolling_mean_3h'] = df.groupby('zone_id')['order_count'].transform(lambda x: x.shift(1).rolling(3).mean())
    df['rolling_mean_24h'] = df.groupby('zone_id')['order_count'].transform(lambda x: x.shift(1).rolling(24).mean())
    df['rolling_std_3h'] = df.groupby('zone_id')['order_count'].transform(lambda x: x.shift(1).rolling(3).std())
    
    # Fill rolling std NaNs with 0
    df['rolling_std_3h'] = df['rolling_std_3h'].fillna(0.0)
    
    # Event features
    df['weather_rainy'] = (df['weather_condition'] == 'rainy').astype(int)
    df['is_peak_hour'] = df['hour_of_day'].isin([12, 13, 14, 19, 20, 21, 22]).astype(int)
    
    # Zone encoding
    zone_map = {"Z1": 0, "Z2": 1, "Z3": 2, "Z4": 3, "Z5": 4}
    df['zone_id_encoded'] = df['zone_id'].map(zone_map)
    
    # Save to final temp CSV
    out_file = os.path.join(TEMP_DIR, "final_features.csv")
    df.to_csv(out_file, index=False)
    print(f"Lag features written to {out_file}")
    
    context['ti'].xcom_push(key='temp_file', value=out_file)
    return out_file

def write_features_fn(**context):
    """Writes final features to database and cleans up temp files."""
    temp_file = context['ti'].xcom_pull(task_ids='engineer_lag_features', key='temp_file')
    if not temp_file or not os.path.exists(temp_file):
        raise FileNotFoundError("Final features temp file not found.")
        
    df = pd.read_csv(temp_file)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    engine = get_db_engine()
    
    # Truncate and reload features
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE features;")
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Failed to truncate features table: {e}")
    finally:
        conn.close()
        
    df_db = df.drop(columns=['weather_condition'])
    print(f"Writing {len(df_db)} rows to database 'features' table...")
    df_db.to_sql("features", engine, if_exists="append", index=False, chunksize=10000)
    
    # Log pipeline run
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO pipeline_runs (dag_name, status, rows_processed) VALUES (%s, %s, %s)",
                ('feature_engineering', 'SUCCESS', len(df))
            )
        conn.commit()
    finally:
        conn.close()
        
    # Clean up temp files
    try:
        for f in ["aggregated.csv", "time_features.csv", "final_features.csv"]:
            fp = os.path.join(TEMP_DIR, f)
            if os.path.exists(fp):
                os.remove(fp)
        print("Cleaned up temporary CSV files.")
    except Exception as e:
        print(f"Error cleaning up temp files: {e}")
        
    return f"Wrote {len(df)} feature rows."

def on_failure_callback(context):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO pipeline_runs (dag_name, status, rows_processed) VALUES (%s, %s, %s)",
                ('feature_engineering', 'FAILED', 0)
            )
        conn.commit()
    finally:
        conn.close()

with DAG(
    'feature_engineering',
    default_args=default_args,
    description='Engineers features for model training and predictions',
    schedule='15 * * * *',
    catchup=False,
    on_failure_callback=on_failure_callback
) as dag:

    aggregate_hourly = PythonOperator(
        task_id='aggregate_hourly',
        python_callable=aggregate_hourly_fn,
    )

    engineer_time_features = PythonOperator(
        task_id='engineer_time_features',
        python_callable=engineer_time_features_fn,
    )

    engineer_lag_features = PythonOperator(
        task_id='engineer_lag_features',
        python_callable=engineer_lag_features_fn,
    )

    write_features = PythonOperator(
        task_id='write_features',
        python_callable=write_features_fn,
    )

    # Sequential dependencies
    aggregate_hourly >> engineer_time_features >> engineer_lag_features >> write_features
