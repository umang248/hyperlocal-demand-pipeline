import os
import sys
# Add scripts directory to path
sys.path.append(os.path.dirname(__file__))

import joblib
import pandas as pd
import numpy as np
from datetime import datetime
from lightgbm import LGBMRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error
from db_utils import get_db_engine, get_db_connection

def calculate_mape(actual, predicted):
    """Calculates Mean Absolute Percentage Error, avoiding division by zero."""
    mask = actual > 0
    if not np.any(mask):
        return 0.0
    return np.mean(np.abs(actual[mask] - predicted[mask]) / actual[mask]) * 100

def train_and_evaluate():
    print("Starting model training process...")
    engine = get_db_engine()
    
    # 1. Load features from database
    print("Loading features...")
    df = pd.read_sql("SELECT * FROM features ORDER BY zone_id, timestamp", engine)
    
    if df.empty:
        print("Features table is empty. Please run feature engineering first.")
        return
        
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Drop rows where lag features are null (first 168 hours of the dataset)
    # This ensures the model trains on fully populated rows.
    df_clean = df.dropna(subset=['lag_168h']).copy()
    
    if len(df_clean) < 1000:
        print(f"Not enough data for training (found {len(df_clean)} rows after dropping NaNs).")
        return
        
    # Split into train (first 5 months, before 2026-06-01) and val (last 1 month, starting 2026-06-01)
    split_date = pd.to_datetime("2026-06-01")
    train_df = df_clean[df_clean['timestamp'] < split_date]
    val_df = df_clean[df_clean['timestamp'] >= split_date]
    
    print(f"Train rows: {len(train_df)} (before {split_date})")
    print(f"Validation rows: {len(val_df)} (after {split_date})")
    
    if train_df.empty or val_df.empty:
        print("Training or validation set is empty. Check simulation dates.")
        return
        
    # Define features and target
    feature_cols = [
        'hour_of_day', 'day_of_week', 'is_weekend', 'month', 'week_of_year',
        'lag_1h', 'lag_2h', 'lag_24h', 'lag_168h',
        'rolling_mean_3h', 'rolling_mean_24h', 'rolling_std_3h',
        'is_festival_day', 'weather_rainy', 'is_peak_hour', 'zone_id_encoded'
    ]
    target_col = 'order_count'
    
    X_train = train_df[feature_cols]
    y_train = train_df[target_col]
    X_val = val_df[feature_cols]
    y_val = val_df[target_col]
    
    # 2. Train LightGBM model with fixed hyperparameters
    print("Training LightGBM model...")
    model = LGBMRegressor(
        n_estimators=200,
        learning_rate=0.05,
        max_depth=6,
        num_leaves=31,
        random_state=42,
        verbosity=-1
    )
    
    model.fit(X_train, y_train)
    print("Model training complete.")
    
    # 3. Evaluate model on validation set
    y_pred = model.predict(X_val)
    # Clip predictions to 0 since order counts cannot be negative
    y_pred = np.clip(y_pred, 0, None)
    
    mape = calculate_mape(y_val.values, y_pred)
    rmse = np.sqrt(mean_squared_error(y_val, y_pred))
    mae = mean_absolute_error(y_val, y_pred)
    
    print(f"Validation Metrics: MAPE: {mape:.2f}%, RMSE: {rmse:.2f}, MAE: {mae:.2f}")
    
    # 4. Compare with naive baseline (predict last week same hour: lag_168h)
    baseline_pred = val_df['lag_168h'].values
    baseline_mape = calculate_mape(y_val.values, baseline_pred)
    
    print(f"Naive Baseline (lag_168h) MAPE: {baseline_mape:.2f}%")
    
    promoted = mape < baseline_mape
    print(f"Does model beat baseline? {promoted}")
    
    # Create models directory if not exists
    os.makedirs("models", exist_ok=True)
    
    # Save model artifact
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_filename = f"lgbm_{timestamp_str}.pkl"
    model_path = os.path.join("models", model_filename)
    
    joblib.dump(model, model_path)
    print(f"Saved model artifact to {model_path}")
    
    if promoted:
        # Update latest.txt pointer
        latest_path = os.path.join("models", "latest.txt")
        with open(latest_path, "w") as f:
            f.write(model_filename)
        print(f"Updated {latest_path} to point to {model_filename}")
    else:
        print("Model did not beat baseline. Not promoting to latest.")
        
    # 5. Log metrics to model_runs table in PostgreSQL
    run_id = f"run_{timestamp_str}"
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            insert_query = """
                INSERT INTO model_runs (run_id, train_rows, val_rows, mape, rmse, mae, model_path, promoted, baseline_mape)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (
                run_id,
                int(len(train_df)),
                int(len(val_df)),
                float(mape),
                float(rmse),
                float(mae),
                model_path,
                bool(promoted),
                float(baseline_mape)
            ))
        conn.commit()
        print("Logged model run metrics to database.")
    except Exception as e:
        conn.rollback()
        print(f"Error logging model run: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    train_and_evaluate()
