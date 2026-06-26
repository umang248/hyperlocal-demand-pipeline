import os
import uuid
import random
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import sys
# Add scripts directory to path
sys.path.append(os.path.dirname(__file__))

from db_utils import get_db_engine

def generate_simulation_data():
    print("Starting data simulation...")
    np.random.seed(42)
    random.seed(42)
    
    start_date = date(2026, 1, 1)
    end_date = date(2026, 6, 30)
    delta_days = (end_date - start_date).days + 1
    
    # 15 festival dates in Bangalore/India across 6 months of 2026
    festival_dates = {
        date(2026, 1, 1),   # New Year
        date(2026, 1, 14),  # Makar Sankranti
        date(2026, 1, 26),  # Republic Day
        date(2026, 2, 14),  # Valentine's Day
        date(2026, 3, 8),   # Maha Shivratri / Holi season
        date(2026, 3, 25),  # Ugadi
        date(2026, 4, 2),   # Good Friday / Easter
        date(2026, 4, 10),  # Eid-ul-Fitr
        date(2026, 4, 14),  # Ambedkar Jayanti
        date(2026, 5, 1),   # May Day
        date(2026, 5, 10),  # Mother's Day / Spring Festival
        date(2026, 6, 1),   # Local Festival
        date(2026, 6, 15),  # Monsoon Festival
        date(2026, 6, 20),  # Weekend Festival
        date(2026, 6, 25),  # Mid-Year Feast
    }
    
    zones = ["Z1", "Z2", "Z3", "Z4", "Z5"]
    
    # Zone volume multipliers: Z1 has 40% more orders than Z5 (base 1.0)
    zone_multipliers = {
        "Z1": 1.4,
        "Z2": 1.3,
        "Z3": 1.2,
        "Z4": 1.1,
        "Z5": 1.0
    }
    
    # Zone delivery time base means (mins)
    zone_delivery_means = {
        "Z1": 25.0, # dense urban
        "Z2": 20.0,
        "Z3": 18.0,
        "Z4": 30.0,
        "Z5": 35.0  # suburban, longer distances
    }
    
    base_hourly_orders = 8.0  # overall baseline orders per hour per zone
    
    orders = []
    
    current_date = start_date
    for day_idx in range(delta_days):
        day_of_week = current_date.weekday() # 0 = Monday, 6 = Sunday
        is_weekend = day_of_week >= 5
        is_fest = current_date in festival_dates
        
        # Determine weather for the day (monsoon season starts mid-May in Bangalore)
        # We will roll weather per hour, but with day-level tendencies
        day_rain_prob = 0.35 if current_date.month in [5, 6] else 0.10
        
        for hour in range(24):
            # Roll weather condition for this hour
            rand_val = random.random()
            if rand_val < day_rain_prob:
                weather = "rainy"
            elif rand_val < day_rain_prob + 0.25:
                weather = "cloudy"
            else:
                weather = "sunny"
                
            for zone in zones:
                # Calculate multipliers
                # Peak hours: 12-2 PM (12, 13, 14) and 7-10 PM (19, 20, 21, 22) -> 2.5x to 3x
                if hour in [12, 13, 14, 19, 20, 21, 22]:
                    hour_mult = random.uniform(2.5, 3.0)
                elif hour in [1, 2, 3, 4, 5]: # late night/early morning slump
                    hour_mult = 0.15
                else:
                    hour_mult = 1.0
                    
                weekend_mult = 1.4 if is_weekend else 1.0
                fest_mult = random.uniform(2.0, 3.0) if is_fest else 1.0
                rain_mult = 1.6 if weather == "rainy" else 1.0
                zone_mult = zone_multipliers[zone]
                
                # Apply noise: +/- 15%
                noise_mult = random.uniform(0.85, 1.15)
                
                expected_orders = base_hourly_orders * hour_mult * weekend_mult * fest_mult * rain_mult * zone_mult * noise_mult
                
                # Draw number of orders from Poisson distribution
                num_orders = np.random.poisson(expected_orders)
                
                for _ in range(num_orders):
                    # Randomize time within the hour
                    minute = random.randint(0, 59)
                    second = random.randint(0, 59)
                    order_time = datetime(
                        current_date.year, current_date.month, current_date.day,
                        hour, minute, second
                    )
                    
                    # Right-skewed order value: base 150 + exponential, capped at 1500
                    order_val = 150.0 + np.random.exponential(scale=300.0)
                    order_val = float(np.clip(order_val, 150.0, 1500.0))
                    
                    # Items count: 1 to 12 items (Poisson-like)
                    items = int(np.clip(np.random.poisson(3.5) + 1, 1, 12))
                    
                    # Zone-dependent delivery time
                    deliv_mean = zone_delivery_means[zone]
                    delivery_time = deliv_mean + np.random.normal(0, 5.0)
                    delivery_time = float(np.clip(delivery_time, 8.0, 45.0))
                    
                    orders.append({
                        "order_id": str(uuid.uuid4()),
                        "zone_id": zone,
                        "timestamp": order_time,
                        "order_value": round(order_val, 2),
                        "items_count": items,
                        "delivery_time_mins": round(delivery_time, 1),
                        "is_weekend": is_weekend,
                        "weather_condition": weather,
                        "is_festival_day": is_fest
                    })
                    
        current_date += timedelta(days=1)
        
    print(f"Generated {len(orders)} orders.")
    
    # Create DataFrame
    df = pd.DataFrame(orders)
    
    # Write to raw_orders table in database
    engine = get_db_engine()
    print("Writing simulated data to PostgreSQL 'raw_orders' table (this may take a few seconds)...")
    df.to_sql("raw_orders", engine, if_exists="append", index=False, chunksize=10000)
    print("Data simulation and ingestion complete!")
    
    # Print some stats
    print("\nSimulated Dataset Summary:")
    print(f"Total Rows: {len(df)}")
    print(f"Date Range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    print(f"Orders per Zone:\n{df['zone_id'].value_counts().to_string()}")
    print(f"Avg Order Value: Rs. {df['order_value'].mean():.2f}")
    print(f"Avg Items per Order: {df['items_count'].mean():.2f}")
    print(f"Avg Delivery Time: {df['delivery_time_mins'].mean():.2f} mins")

if __name__ == "__main__":
    generate_simulation_data()
