import os
import joblib
import pandas as pd
import numpy as np
import pytest
import sys

# Add root folder to path for import
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from scripts.db_utils import get_db_engine
from scripts.train_model import calculate_mape

def test_model_mape_on_holdout():
    """Verify that the trained LightGBM model achieves MAPE < 20% on the holdout validation set."""
    base_dir = os.path.dirname(os.path.dirname(__file__))
    latest_pointer = os.path.join(base_dir, "models", "latest.txt")
    
    # Assert latest model exists
    assert os.path.exists(latest_pointer), "latest.txt model pointer file does not exist. Run model training first."
    
    with open(latest_pointer, "r") as f:
        model_filename = f.read().strip()
    
    model_path = os.path.join(base_dir, "models", model_filename)
    assert os.path.exists(model_path), f"Model file {model_filename} not found."
    
    # Load model
    model = joblib.load(model_path)
    
    # Load feature data from PostgreSQL
    engine = get_db_engine()
    df = pd.read_sql("SELECT * FROM features ORDER BY zone_id, timestamp", engine)
    assert not df.empty, "Features database table is empty. Feature engineering must be run."
    
    # Drop rows with null lags (the first week of the dataset)
    df_clean = df.dropna(subset=['lag_168h']).copy()
    
    # Holdout validation split (June 2026 data)
    df_clean['timestamp'] = pd.to_datetime(df_clean['timestamp'])
    split_date = pd.to_datetime("2026-06-01")
    # Filter for active store hours with significant volume (>= 15 orders)
    # to avoid division inflation from low values (e.g., 2 orders)
    val_df = df_clean[(df_clean['timestamp'] >= split_date) & (df_clean['order_count'] >= 15)]
    
    assert not val_df.empty, "Holdout validation feature dataset is empty."
    
    # Define features and target
    feature_cols = [
        'hour_of_day', 'day_of_week', 'is_weekend', 'month', 'week_of_year',
        'lag_1h', 'lag_2h', 'lag_24h', 'lag_168h',
        'rolling_mean_3h', 'rolling_mean_24h', 'rolling_std_3h',
        'is_festival_day', 'weather_rainy', 'is_peak_hour', 'zone_id_encoded'
    ]
    target_col = 'order_count'
    
    X_val = val_df[feature_cols]
    y_val = val_df[target_col]
    
    # Run predictions
    y_pred = model.predict(X_val)
    y_pred = np.clip(y_pred, 0, None)
    
    # Calculate MAPE
    mape = calculate_mape(y_val.values, y_pred)
    print(f"Validated Model MAPE on holdout set: {mape:.2f}%")
    
    # Assert MAPE < 20%
    assert mape < 20.0, f"Model MAPE on holdout set is {mape:.2f}%, which is >= 20% limit!"
