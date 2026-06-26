import pandas as pd
import numpy as np
from db_utils import get_db_engine, get_db_connection

def run_feature_engineering():
    print("Running feature engineering pipeline...")
    engine = get_db_engine()
    
    # 1. Read all clean orders
    print("Loading clean orders from database...")
    orders_df = pd.read_sql("SELECT * FROM clean_orders", engine)
    
    if orders_df.empty:
        print("No clean orders found. Skipping feature engineering.")
        return
        
    orders_df['timestamp'] = pd.to_datetime(orders_df['timestamp'])
    
    # Floor timestamps to the hour
    orders_df['hour_timestamp'] = orders_df['timestamp'].dt.floor('h')
    
    # 2. Hourly aggregation per zone
    agg_df = orders_df.groupby(['zone_id', 'hour_timestamp']).agg(
        order_count=('order_id', 'count'),
        is_festival_day=('is_festival_day', 'max'),
        weather_condition=('weather_condition', lambda x: x.mode()[0] if not x.empty else 'sunny')
    ).reset_index()
    
    # Rename hour_timestamp to timestamp
    agg_df = agg_df.rename(columns={'hour_timestamp': 'timestamp'})
    
    # 3. Reindex to include a complete hourly grid for all zones to avoid time-series gaps
    min_time = agg_df['timestamp'].min()
    max_time = agg_df['timestamp'].max()
    print(f"Creating complete hourly grid from {min_time} to {max_time}...")
    
    all_hours = pd.date_range(start=min_time, end=max_time, freq='h')
    all_zones = ["Z1", "Z2", "Z3", "Z4", "Z5"]
    
    grid = pd.MultiIndex.from_product([all_zones, all_hours], names=['zone_id', 'timestamp']).to_frame().reset_index(drop=True)
    
    # Merge aggregated data onto grid
    df = pd.merge(grid, agg_df, on=['zone_id', 'timestamp'], how='left')
    
    # Fill missing values
    df['order_count'] = df['order_count'].fillna(0).astype(int)
    
    # For weather and festival_day, we resolve by looking at hour-level data across all zones
    # (since weather and festivals are metropolitan-wide)
    hour_metadata = agg_df.groupby('timestamp').agg(
        hour_weather=('weather_condition', lambda x: x.mode()[0] if not x.empty else 'sunny'),
        hour_fest=('is_festival_day', 'max')
    ).reset_index()
    
    df = pd.merge(df, hour_metadata, on='timestamp', how='left')
    df['weather_condition'] = df['weather_condition'].fillna(df['hour_weather']).fillna('sunny')
    df['is_festival_day'] = df['is_festival_day'].fillna(df['hour_fest']).fillna(False).astype(bool)
    df = df.drop(columns=['hour_weather', 'hour_fest'])
    
    # Sort for time-series computations
    df = df.sort_values(by=['zone_id', 'timestamp']).reset_index(drop=True)
    
    # 4. Time features
    df['hour_of_day'] = df['timestamp'].dt.hour
    df['day_of_week'] = df['timestamp'].dt.dayofweek
    df['is_weekend'] = df['day_of_week'] >= 5
    df['month'] = df['timestamp'].dt.month
    df['week_of_year'] = df['timestamp'].dt.isocalendar().week.astype(int)
    
    # 5. Lag features (per zone)
    print("Computing lag features...")
    df['lag_1h'] = df.groupby('zone_id')['order_count'].shift(1)
    df['lag_2h'] = df.groupby('zone_id')['order_count'].shift(2)
    df['lag_24h'] = df.groupby('zone_id')['order_count'].shift(24)
    df['lag_168h'] = df.groupby('zone_id')['order_count'].shift(168)
    
    # 6. Rolling window features (per zone)
    print("Computing rolling window features...")
    df['rolling_mean_3h'] = df.groupby('zone_id')['order_count'].transform(lambda x: x.shift(1).rolling(3).mean())
    df['rolling_mean_24h'] = df.groupby('zone_id')['order_count'].transform(lambda x: x.shift(1).rolling(24).mean())
    df['rolling_std_3h'] = df.groupby('zone_id')['order_count'].transform(lambda x: x.shift(1).rolling(3).std())
    
    # Fill rolling std NaNs with 0 (where we don't have enough history yet)
    df['rolling_std_3h'] = df['rolling_std_3h'].fillna(0.0)
    
    # 7. Event features
    df['weather_rainy'] = (df['weather_condition'] == 'rainy').astype(int)
    df['is_peak_hour'] = df['hour_of_day'].isin([12, 13, 14, 19, 20, 21, 22]).astype(int)
    
    # 8. Zone encoding
    zone_map = {"Z1": 0, "Z2": 1, "Z3": 2, "Z4": 3, "Z5": 4}
    df['zone_id_encoded'] = df['zone_id'].map(zone_map)
    
    # Drop columns not in database schema
    df_db = df.drop(columns=['weather_condition'])
    
    # Truncate features table before write
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE features;")
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Failed to truncate features: {e}")
    finally:
        conn.close()
        
    print(f"Writing {len(df_db)} feature rows to PostgreSQL 'features' table...")
    df_db.to_sql("features", engine, if_exists="append", index=False, chunksize=10000)
    print("Feature engineering complete!")

if __name__ == "__main__":
    run_feature_engineering()
