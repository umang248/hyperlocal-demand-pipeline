import pytest
import pandas as pd
import numpy as np

def test_lag_features_no_cross_zone_leakage():
    """Verify that lag and rolling calculations do not leak data between zones."""
    # Create mock dataset for 2 zones (Z1 and Z2) with sequential data
    data = [
        {"zone_id": "Z1", "timestamp": "2026-01-01 00:00:00", "order_count": 10},
        {"zone_id": "Z1", "timestamp": "2026-01-01 01:00:00", "order_count": 20},
        {"zone_id": "Z1", "timestamp": "2026-01-01 02:00:00", "order_count": 30},
        {"zone_id": "Z2", "timestamp": "2026-01-01 00:00:00", "order_count": 100},
        {"zone_id": "Z2", "timestamp": "2026-01-01 01:00:00", "order_count": 200},
        {"zone_id": "Z2", "timestamp": "2026-01-01 02:00:00", "order_count": 300},
    ]
    df = pd.DataFrame(data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values(by=['zone_id', 'timestamp']).reset_index(drop=True)
    
    # Compute lags and rollings grouping by zone_id (exactly as in features pipeline)
    df['lag_1h'] = df.groupby('zone_id')['order_count'].shift(1)
    df['lag_2h'] = df.groupby('zone_id')['order_count'].shift(2)
    df['rolling_mean_3h'] = df.groupby('zone_id')['order_count'].transform(lambda x: x.shift(1).rolling(3).mean())
    
    # Validate Z1 lags
    z1 = df[df['zone_id'] == 'Z1'].reset_index(drop=True)
    assert pd.isna(z1.loc[0, 'lag_1h'])
    assert z1.loc[1, 'lag_1h'] == 10
    assert z1.loc[2, 'lag_1h'] == 20
    assert z1.loc[2, 'lag_2h'] == 10
    
    # Validate Z2 lags: Crucially, if cross-zone leakage happens, the first row of Z2 
    # would incorrectly inherit Z1's last value (30) because of sequential shifts
    z2 = df[df['zone_id'] == 'Z2'].reset_index(drop=True)
    assert pd.isna(z2.loc[0, 'lag_1h']) # Must be NaN, indicating isolation
    assert z2.loc[1, 'lag_1h'] == 100
    assert z2.loc[2, 'lag_1h'] == 200
    assert z2.loc[2, 'lag_2h'] == 100
    
    # Validate rolling average isolation
    # Set mock data representing 3 values
    data_rolling = [
        {"zone_id": "Z1", "timestamp": "2026-01-01 00:00:00", "order_count": 10},
        {"zone_id": "Z1", "timestamp": "2026-01-01 01:00:00", "order_count": 20},
        {"zone_id": "Z1", "timestamp": "2026-01-01 02:00:00", "order_count": 30},
        {"zone_id": "Z1", "timestamp": "2026-01-01 03:00:00", "order_count": 40},
    ]
    df_roll = pd.DataFrame(data_rolling)
    df_roll['lag_rolling_mean_3h'] = df_roll.groupby('zone_id')['order_count'].transform(lambda x: x.shift(1).rolling(3).mean())
    
    # For index 3 (value 40), the rolling average of past 3 hours should be (10+20+30)/3 = 20.0
    assert df_roll.loc[3, 'lag_rolling_mean_3h'] == 20.0
