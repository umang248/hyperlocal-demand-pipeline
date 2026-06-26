-- PostgreSQL Database Schema for Hyperlocal Demand Forecasting Pipeline

-- Raw orders as received from simulation or upstream sources
CREATE TABLE IF NOT EXISTS raw_orders (
    order_id VARCHAR(50) PRIMARY KEY,
    zone_id VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    order_value DOUBLE PRECISION NOT NULL,
    items_count INT NOT NULL,
    delivery_time_mins DOUBLE PRECISION NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    weather_condition VARCHAR(20) NOT NULL,
    is_festival_day BOOLEAN NOT NULL
);

-- Clean orders after schema validation and range checks
CREATE TABLE IF NOT EXISTS clean_orders (
    order_id VARCHAR(50) PRIMARY KEY,
    zone_id VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    order_value DOUBLE PRECISION NOT NULL,
    items_count INT NOT NULL,
    delivery_time_mins DOUBLE PRECISION NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    weather_condition VARCHAR(20) NOT NULL,
    is_festival_day BOOLEAN NOT NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index on zone_id and timestamp for fast feature engineering
CREATE INDEX IF NOT EXISTS idx_clean_orders_zone_time ON clean_orders(zone_id, timestamp);

-- Invalidation log for failed records
CREATE TABLE IF NOT EXISTS validation_errors (
    error_id SERIAL PRIMARY KEY,
    order_id VARCHAR(50),
    zone_id VARCHAR(10),
    timestamp TIMESTAMP,
    order_value DOUBLE PRECISION,
    items_count INT,
    delivery_time_mins DOUBLE PRECISION,
    is_weekend BOOLEAN,
    weather_condition VARCHAR(20),
    is_festival_day BOOLEAN,
    error_reason TEXT,
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Feature store aggregated per zone and hour
CREATE TABLE IF NOT EXISTS features (
    zone_id VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    order_count INT NOT NULL,
    hour_of_day INT NOT NULL,
    day_of_week INT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    month INT NOT NULL,
    week_of_year INT NOT NULL,
    lag_1h DOUBLE PRECISION,
    lag_2h DOUBLE PRECISION,
    lag_24h DOUBLE PRECISION,
    lag_168h DOUBLE PRECISION,
    rolling_mean_3h DOUBLE PRECISION,
    rolling_mean_24h DOUBLE PRECISION,
    rolling_std_3h DOUBLE PRECISION,
    is_festival_day BOOLEAN NOT NULL,
    weather_rainy INT NOT NULL,
    is_peak_hour INT NOT NULL,
    zone_id_encoded INT NOT NULL,
    PRIMARY KEY (zone_id, timestamp)
);

-- Tracking model training runs and metadata
CREATE TABLE IF NOT EXISTS model_runs (
    run_id VARCHAR(50) PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    train_rows INT NOT NULL,
    val_rows INT NOT NULL,
    mape DOUBLE PRECISION NOT NULL,
    rmse DOUBLE PRECISION NOT NULL,
    mae DOUBLE PRECISION NOT NULL,
    model_path VARCHAR(255) NOT NULL,
    promoted BOOLEAN NOT NULL,
    baseline_mape DOUBLE PRECISION
);

-- Generated predictions for the next 2 hours
CREATE TABLE IF NOT EXISTS predictions (
    prediction_id SERIAL PRIMARY KEY,
    zone_id VARCHAR(10) NOT NULL,
    forecast_hour TIMESTAMP NOT NULL,
    predicted_order_count DOUBLE PRECISION NOT NULL,
    confidence_flag VARCHAR(20) NOT NULL, -- 'NORMAL', 'HIGH_DEMAND'
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Alerts for high demand zones
CREATE TABLE IF NOT EXISTS alerts (
    alert_id SERIAL PRIMARY KEY,
    zone_id VARCHAR(10) NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    message TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Log table for pipeline runs to show in the Streamlit dashboard
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id SERIAL PRIMARY KEY,
    dag_name VARCHAR(100) NOT NULL,
    run_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) NOT NULL, -- 'SUCCESS', 'FAILED', 'RUNNING'
    rows_processed INT DEFAULT 0
);
