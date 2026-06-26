import os
import sys
import joblib
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

def load_latest_model_fn(**context):
    """Reads models/latest.txt and loads the model pkl."""
    print("Locating latest model artifact...")
    base_dir = os.path.dirname(os.path.dirname(__file__))
    latest_pointer = os.path.join(base_dir, "models", "latest.txt")
    
    if not os.path.exists(latest_pointer):
        # Let's see if we can find any pkl files in models/
        models_dir = os.path.join(base_dir, "models")
        pkl_files = [f for f in os.listdir(models_dir) if f.endswith('.pkl')] if os.path.exists(models_dir) else []
        if not pkl_files:
            raise FileNotFoundError("No models found. Run model training first!")
        else:
            model_filename = sorted(pkl_files)[-1]
            print(f"latest.txt not found, using most recent file: {model_filename}")
    else:
        with open(latest_pointer, "r") as f:
            model_filename = f.read().strip()
            
    model_path = os.path.join(base_dir, "models", model_filename)
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Model file {model_path} not found.")
        
    print(f"Successfully located latest model: {model_path}")
    context['ti'].xcom_push(key='model_path', value=model_path)
    return model_path

def generate_forecasts_fn(**context):
    """Recursive multi-step forecasting for the next 2 hours."""
    model_path = context['ti'].xcom_pull(task_ids='load_latest_model', key='model_path')
    model = joblib.load(model_path)
    print("Loaded model.")
    
    engine = get_db_engine()
    
    # Get the latest timestamp processed in our features table
    print("Fetching historical data from features table...")
    df_history = pd.read_sql("SELECT * FROM features ORDER BY zone_id, timestamp", engine)
    
    if df_history.empty:
        raise ValueError("Features table is empty. Feature engineering must run before predictions.")
        
    df_history['timestamp'] = pd.to_datetime(df_history['timestamp'])
    
    # Latest processed hour T
    max_timestamp = df_history['timestamp'].max()
    print(f"Latest historical hour T: {max_timestamp}")
    
    # 15 festival dates
    festival_dates = {
        "2026-01-01", "2026-01-14", "2026-01-26", "2026-02-14", "2026-03-08",
        "2026-03-25", "2026-04-02", "2026-04-10", "2026-04-14", "2026-05-01",
        "2026-05-10", "2026-06-01", "2026-06-15", "2026-06-20", "2026-06-25"
    }
    
    zones = ["Z1", "Z2", "Z3", "Z4", "Z5"]
    zone_map = {"Z1": 0, "Z2": 1, "Z3": 2, "Z4": 3, "Z5": 4}
    
    predictions_list = []
    
    # We will build predictions for T+1h and T+2h recursively
    # Create copies of histories to append future predictions
    hist_dict = {z: df_history[df_history['zone_id'] == z].copy() for z in zones}
    
    for step in [1, 2]:
        forecast_time = max_timestamp + timedelta(hours=step)
        print(f"\nForecasting step {step} for time: {forecast_time}")
        
        step_predictions = {}
        
        for zone in zones:
            z_df = hist_dict[zone]
            
            # Get latest available actual/predicted order count series
            # Sort by timestamp to ensure correct lags
            z_df = z_df.sort_values(by='timestamp').reset_index(drop=True)
            
            # Construct feature row
            hour_of_day = forecast_time.hour
            day_of_week = forecast_time.weekday()
            is_weekend = day_of_week >= 5
            month = forecast_time.month
            week_of_year = forecast_time.isocalendar()[1]
            
            is_fest = forecast_time.strftime("%Y-%m-%d") in festival_dates
            
            # Forward-fill weather from the latest actual hour T
            latest_weather = z_df.iloc[-1]['weather_condition'] if 'weather_condition' in z_df.columns else 'sunny'
            weather_rainy = 1 if latest_weather == 'rainy' else 0
            
            is_peak = 1 if hour_of_day in [12, 13, 14, 19, 20, 21, 22] else 0
            
            # Lags (using relative positions in the sorted history DataFrame)
            # - lag_1h: 1 hour ago (last row of z_df)
            # - lag_2h: 2 hours ago (second last row of z_df)
            # - lag_24h: 24 hours ago (24th row from end)
            # - lag_168h: 168 hours ago (168th row from end)
            lag_1h = float(z_df.iloc[-1]['order_count'])
            lag_2h = float(z_df.iloc[-2]['order_count']) if len(z_df) >= 2 else lag_1h
            lag_24h = float(z_df.iloc[-24]['order_count']) if len(z_df) >= 24 else lag_1h
            lag_168h = float(z_df.iloc[-168]['order_count']) if len(z_df) >= 168 else lag_1h
            
            # Rolling (using the shifted history values)
            rolling_mean_3h = z_df.iloc[-3:]['order_count'].mean()
            rolling_mean_24h = z_df.iloc[-24:]['order_count'].mean() if len(z_df) >= 24 else z_df['order_count'].mean()
            rolling_std_3h = z_df.iloc[-3:]['order_count'].std()
            if pd.isna(rolling_std_3h):
                rolling_std_3h = 0.0
                
            zone_encoded = zone_map[zone]
            
            # Assemble feature vector matching LightGBM training features order
            # ['hour_of_day', 'day_of_week', 'is_weekend', 'month', 'week_of_year',
            #  'lag_1h', 'lag_2h', 'lag_24h', 'lag_168h',
            #  'rolling_mean_3h', 'rolling_mean_24h', 'rolling_std_3h',
            #  'is_festival_day', 'weather_rainy', 'is_peak_hour', 'zone_id_encoded']
            features = pd.DataFrame([{
                'hour_of_day': hour_of_day,
                'day_of_week': day_of_week,
                'is_weekend': is_weekend,
                'month': month,
                'week_of_year': week_of_year,
                'lag_1h': lag_1h,
                'lag_2h': lag_2h,
                'lag_24h': lag_24h,
                'lag_168h': lag_168h,
                'rolling_mean_3h': rolling_mean_3h,
                'rolling_mean_24h': rolling_mean_24h,
                'rolling_std_3h': rolling_std_3h,
                'is_festival_day': is_fest,
                'weather_rainy': weather_rainy,
                'is_peak_hour': is_peak,
                'zone_id_encoded': zone_encoded
            }])
            
            # Run prediction
            pred_val = model.predict(features)[0]
            pred_val = max(0.0, float(pred_val)) # Clip negative forecasts
            
            step_predictions[zone] = pred_val
            
            # Append forecast row to our history dict so it acts as lags/rollings for next step!
            new_hist_row = {
                'zone_id': zone,
                'timestamp': forecast_time,
                'order_count': pred_val,
                'weather_condition': latest_weather,
                'is_festival_day': is_fest,
                'hour_of_day': hour_of_day,
                'day_of_week': day_of_week,
                'is_weekend': is_weekend,
                'month': month,
                'week_of_year': week_of_year,
                'lag_1h': lag_1h,
                'lag_2h': lag_2h,
                'lag_24h': lag_24h,
                'lag_168h': lag_168h,
                'rolling_mean_3h': rolling_mean_3h,
                'rolling_mean_24h': rolling_mean_24h,
                'rolling_std_3h': rolling_std_3h,
                'weather_rainy': weather_rainy,
                'is_peak_hour': is_peak,
                'zone_id_encoded': zone_encoded
            }
            hist_dict[zone] = pd.concat([z_df, pd.DataFrame([new_hist_row])], ignore_index=True)
            
            # Store in predictions list
            predictions_list.append({
                "zone_id": zone,
                "forecast_hour": forecast_time,
                "predicted_order_count": round(pred_val, 2),
                "confidence_flag": "NORMAL" # Will be updated in alerts check
            })
            
    # Write to database predictions table
    pred_df = pd.DataFrame(predictions_list)
    print(f"Writing {len(pred_df)} predictions to database...")
    pred_df.to_sql("predictions", engine, if_exists="append", index=False)
    
    # Save the output file path for alerts checking task
    temp_preds_path = os.path.join(TEMP_DIR, "latest_predictions.csv")
    pred_df.to_csv(temp_preds_path, index=False)
    context['ti'].xcom_push(key='temp_preds_path', value=temp_preds_path)
    
    return temp_preds_path

def flag_high_demand_zones_fn(**context):
    """Raises alert if predicted demand > 1.5x of 7-day average."""
    temp_preds_path = context['ti'].xcom_pull(task_ids='generate_forecasts', key='temp_preds_path')
    if not temp_preds_path or not os.path.exists(temp_preds_path):
        raise FileNotFoundError("Predictions temp file not found.")
        
    pred_df = pd.read_csv(temp_preds_path)
    pred_df['forecast_hour'] = pd.to_datetime(pred_df['forecast_hour'])
    
    engine = get_db_engine()
    
    alerts_raised = 0
    
    for idx, row in pred_df.iterrows():
        zone = row['zone_id']
        fc_hour = row['forecast_hour']
        pred_val = row['predicted_order_count']
        
        # Calculate 7-day average for this zone from features table
        avg_query = """
            SELECT AVG(order_count) as avg_orders 
            FROM features 
            WHERE zone_id = %s 
              AND timestamp >= (SELECT MAX(timestamp) FROM features) - INTERVAL '7 days'
        """
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(avg_query, (zone,))
                res = cursor.fetchone()
                avg_7d = float(res[0]) if res and res[0] is not None else 5.0
        finally:
            conn.close()
            
        print(f"Zone {zone} Forecast: {pred_val:.1f} orders. 7-Day Avg: {avg_7d:.1f} orders.")
        
        if pred_val > 1.5 * avg_7d:
            # Raise alert!
            print(f"WARNING: HIGH DEMAND DETECTED: Zone {zone} at {fc_hour} (Forecast: {pred_val:.1f} > 1.5x Avg: {avg_7d:.1f})")
            
            # Write alert to database
            alert_msg = f"Predicted demand {pred_val:.1f} is {pred_val/avg_7d:.1f}x higher than 7-day average ({avg_7d:.1f})"
            conn = get_db_connection()
            try:
                with conn.cursor() as cursor:
                    # Insert alert
                    cursor.execute(
                        "INSERT INTO alerts (zone_id, alert_type, message) VALUES (%s, %s, %s)",
                        (zone, "HIGH_DEMAND", alert_msg)
                    )
                    # Update predictions confidence flag to HIGH_DEMAND
                    cursor.execute(
                        "UPDATE predictions SET confidence_flag = 'HIGH_DEMAND' WHERE zone_id = %s AND forecast_hour = %s",
                        (zone, fc_hour)
                    )
                conn.commit()
                alerts_raised += 1
            except Exception as e:
                conn.rollback()
                print(f"Error saving alert: {e}")
            finally:
                conn.close()
                
    # Log successful pipeline execution
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO pipeline_runs (dag_name, status, rows_processed) VALUES (%s, %s, %s)",
                ('generate_predictions', 'SUCCESS', len(pred_df))
            )
        conn.commit()
    finally:
        conn.close()
        
    # Clean up temp file
    if os.path.exists(temp_preds_path):
        os.remove(temp_preds_path)
        
    return f"Processed {len(pred_df)} predictions. Raised {alerts_raised} alerts."

def on_failure_callback(context):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO pipeline_runs (dag_name, status, rows_processed) VALUES (%s, %s, %s)",
                ('generate_predictions', 'FAILED', 0)
            )
        conn.commit()
    finally:
        conn.close()

with DAG(
    'generate_predictions',
    default_args=default_args,
    description='Loads latest model and generates next 2-hour forecasts per zone',
    schedule='30 * * * *',
    catchup=False,
    on_failure_callback=on_failure_callback
) as dag:

    load_latest_model = PythonOperator(
        task_id='load_latest_model',
        python_callable=load_latest_model_fn,
    )

    generate_forecasts = PythonOperator(
        task_id='generate_forecasts',
        python_callable=generate_forecasts_fn,
    )

    flag_high_demand_zones = PythonOperator(
        task_id='flag_high_demand_zones',
        python_callable=flag_high_demand_zones_fn,
    )

    # Dependencies
    load_latest_model >> generate_forecasts >> flag_high_demand_zones
