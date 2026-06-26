# MASTER PROJECT PROMPT
## Hyperlocal Demand Forecasting Pipeline — Dark Store Edition

---

## CONTEXT & BACKGROUND

You are helping build a **portfolio data engineering project** targeted at food-tech/hyperlocal companies like Swiggy, Blinkit, and Zepto. The builder is a second-year undergraduate at IIT Kharagpur with strong Python, Pandas, Scikit-learn, and Streamlit skills. They are learning **Apache Airflow for the first time** — this is intentional and is the primary technical depth signal of the project.

The project must:
- Be **end-to-end**: data simulation → ingestion → feature engineering → model training → prediction → dashboard
- Use **Apache Airflow** as the core orchestration layer (4 DAGs)
- Use only **beginner-to-intermediate level algorithms** (no deep learning, no complex frameworks)
- Be **fully reproducible** from a single repo clone
- Produce a **Streamlit dashboard** as the user-facing output
- Mimic a **real-world dark store ops problem** that Blinkit/Zepto actually solves

---

## PROBLEM STATEMENT

Dark stores (micro-warehouses used by Blinkit, Zepto, Swiggy Instamart) need to forecast **how many orders will arrive in the next 1–2 hours per delivery zone** so they can:
- Pre-pack popular SKUs
- Allocate delivery partners
- Trigger restocking from the main warehouse

This project builds the **data pipeline that powers this forecast** — not just the model, but the full orchestrated system around it.

---

## PROJECT ARCHITECTURE

```
[Data Simulation Script]
        ↓
  PostgreSQL Database (raw_orders table)
        ↓
[Airflow DAG 1: ingest_and_validate]
   - Reads new raw order records
   - Validates schema, nulls, range checks
   - Writes clean records to clean_orders table
        ↓
[Airflow DAG 2: feature_engineering]
   - Reads clean_orders
   - Engineers features (see feature list below)
   - Writes to features table
        ↓
[Airflow DAG 3: model_training]
   - Triggered when new data crosses a row threshold OR model MAPE > 15%
   - Trains a LightGBM or XGBoost regressor
   - Saves model artifact to /models/ directory with timestamp
   - Logs metrics to a model_runs table in PostgreSQL
        ↓
[Airflow DAG 4: generate_predictions]
   - Loads latest model artifact
   - Generates next 2-hour demand forecast per zone
   - Writes predictions to predictions table
        ↓
[Streamlit Dashboard]
   - Ops-facing UI showing predicted demand heatmap by zone and hour
   - Model performance tracker
   - Pipeline health status
```

---

## DATASET SIMULATION

### File: `scripts/simulate_data.py`

Generate **6 months of synthetic hourly order data** for **5 dark store zones** in a metro city (e.g., Bangalore).

#### Schema — `raw_orders` table:

| Column | Type | Description |
|---|---|---|
| `order_id` | STRING | UUID, unique per order |
| `zone_id` | STRING | One of: Z1, Z2, Z3, Z4, Z5 |
| `timestamp` | DATETIME | Order placed time |
| `order_value` | FLOAT | ₹150–₹1500, right-skewed |
| `items_count` | INT | 1–12 items |
| `delivery_time_mins` | FLOAT | 8–45 mins, zone-dependent |
| `is_weekend` | BOOL | Derived from timestamp |
| `weather_condition` | STRING | sunny / rainy / cloudy (randomly assigned with weights) |
| `is_festival_day` | BOOL | Mark ~15 dates across 6 months as festival days |

#### Simulation Rules (inject realistic patterns):
- **Peak hours:** 12–2 PM and 7–10 PM should have 2.5x–3x order volume
- **Weekend uplift:** 1.4x multiplier on Saturday and Sunday
- **Festival day uplift:** 2x–3x multiplier on marked festival dates
- **Rainy day uplift:** 1.6x multiplier (people order more when it rains)
- **Zone variation:** Z1 (high-density urban) should have 40% more orders than Z5 (suburban)
- **Add noise:** ±15% random variation to all patterns

---

## FEATURE ENGINEERING

### File: `dags/feature_engineering_dag.py` (also `scripts/feature_engineering.py`)

Aggregate raw orders into **hourly buckets per zone**, then engineer the following features:

#### Target Variable:
- `order_count` — number of orders in that zone in that hour

#### Time Features:
- `hour_of_day` — 0–23
- `day_of_week` — 0 (Monday) to 6 (Sunday)
- `is_weekend` — binary
- `month` — 1–12
- `week_of_year` — 1–52

#### Lag Features (most important for time-series):
- `lag_1h` — order count from 1 hour ago, same zone
- `lag_2h` — order count from 2 hours ago, same zone
- `lag_24h` — order count from same hour yesterday, same zone
- `lag_168h` — order count from same hour last week, same zone

#### Rolling Window Features:
- `rolling_mean_3h` — 3-hour rolling average, same zone
- `rolling_mean_24h` — 24-hour rolling average, same zone
- `rolling_std_3h` — 3-hour rolling std dev (volatility signal)

#### Event Features:
- `is_festival_day` — binary
- `weather_rainy` — binary (1 if rainy, else 0)
- `is_peak_hour` — binary (1 if hour in [12,13,14,19,20,21,22])

#### Zone Feature:
- `zone_id_encoded` — label encoded zone ID

---

## AIRFLOW DAG SPECIFICATIONS

### Setup
- Use **Apache Airflow 2.x** (local installation via pip or Docker)
- All DAGs live in the `/dags/` directory
- Use **PostgreSQL** as both the data store and Airflow's metadata DB (keeps setup simple)
- Schedule all DAGs using cron expressions

---

### DAG 1: `ingest_and_validate`
- **Schedule:** `0 * * * *` (every hour)
- **Tasks:**
  1. `check_new_data` — PythonOperator: check if new rows exist in raw_orders since last run (use Airflow Variables to store last processed timestamp)
  2. `validate_schema` — PythonOperator: check for nulls, correct dtypes, value ranges
  3. `validate_ranges` — PythonOperator: assert order_value > 0, items_count between 1–20, zone_id in valid list
  4. `write_clean_data` — PythonOperator: write validated rows to clean_orders table, log rejected rows to a validation_errors table
- **Failure handling:** If validation fails, send an alert (print to Airflow logs, optionally email)
- **Dependencies:** Task 2 and 3 run in parallel after Task 1; Task 4 runs after both pass

---

### DAG 2: `feature_engineering`
- **Schedule:** `15 * * * *` (15 mins after ingest)
- **Tasks:**
  1. `aggregate_hourly` — PythonOperator: group clean_orders by zone + hour → order_count
  2. `engineer_time_features` — PythonOperator: add all time-based features
  3. `engineer_lag_features` — PythonOperator: compute lag and rolling features (handle NaNs from early rows)
  4. `write_features` — PythonOperator: write final feature table to PostgreSQL
- **Key engineering note:** Lag features must be computed **per zone** — do not leak across zones

---

### DAG 3: `model_training`
- **Schedule:** `0 2 * * *` (2 AM daily — retrain on fresh day of data)
- **Trigger rule:** Also include a sensor that triggers retraining if MAPE on last 24h predictions > 15%
- **Tasks:**
  1. `load_training_data` — PythonOperator: load features table, split into train (first 5 months) / validation (last 1 month)
  2. `train_model` — PythonOperator: train LightGBM regressor with these fixed hyperparameters (no AutoML — manual tuning is the point):
     - `n_estimators=200`, `learning_rate=0.05`, `max_depth=6`, `num_leaves=31`
  3. `evaluate_model` — PythonOperator: compute MAPE, RMSE, MAE on validation set per zone; log to model_runs table
  4. `save_model_artifact` — PythonOperator: save model as `models/lgbm_YYYYMMDD_HHMMSS.pkl` using joblib; update a `models/latest.txt` pointer file
  5. `compare_with_baseline` — PythonOperator: compare new model MAPE against a naive baseline (predict last week same hour). Only promote if new model beats baseline.
- **model_runs table schema:** run_id, timestamp, train_rows, val_rows, mape, rmse, mae, model_path, promoted (bool)

---

### DAG 4: `generate_predictions`
- **Schedule:** `30 * * * *` (30 mins after ingest, after features are ready)
- **Tasks:**
  1. `load_latest_model` — PythonOperator: read `models/latest.txt`, load corresponding .pkl
  2. `prepare_forecast_features` — PythonOperator: build feature rows for the **next 2 hours** for all 5 zones (use known future values: hour, day, festival flag; use last known lag values)
  3. `generate_forecasts` — PythonOperator: run model.predict(), write to predictions table
  4. `flag_high_demand_zones` — PythonOperator: if any zone's predicted demand > 1.5x its 7-day average, write a flag to an alerts table
- **predictions table schema:** prediction_id, zone_id, forecast_hour, predicted_order_count, confidence_flag, generated_at

---

## STREAMLIT DASHBOARD

### File: `app/dashboard.py`

Build a **3-tab Streamlit dashboard** that reads directly from the PostgreSQL database.

---

#### Tab 1: 🗺️ Demand Forecast Map
- **Zone demand heatmap** — horizontal bar chart (one bar per zone) showing predicted order count for the next hour and next 2 hours
- Color coding: green (normal), amber (1.2x avg), red (1.5x+ avg — high demand alert)
- Dropdown to select forecast horizon: next 1 hour / next 2 hours
- Last updated timestamp shown at top

#### Tab 2: 📈 Model Performance
- Line chart: MAPE over last 30 model runs (from model_runs table)
- Table: last 10 runs with columns — date, MAPE, RMSE, train rows, promoted (yes/no)
- Baseline comparison bar: "Model MAPE vs Naive Baseline MAPE" side-by-side

#### Tab 3: 🔧 Pipeline Health
- Table showing last run status of each of the 4 DAGs (read from Airflow's metadata DB or a separate pipeline_runs log table you maintain)
- Columns: DAG name, last run time, status (success/failed/running), rows processed
- Simple alert box at top: "All systems operational ✅" or "⚠️ DAG X failed at HH:MM"

---

## FOLDER STRUCTURE

```
hyperlocal-demand-pipeline/
│
├── dags/
│   ├── ingest_and_validate_dag.py
│   ├── feature_engineering_dag.py
│   ├── model_training_dag.py
│   └── generate_predictions_dag.py
│
├── scripts/
│   ├── simulate_data.py         # One-time data generation
│   ├── feature_engineering.py  # Reusable functions (imported by DAG)
│   ├── train_model.py           # Reusable functions (imported by DAG)
│   └── db_utils.py              # PostgreSQL connection helpers
│
├── app/
│   └── dashboard.py             # Streamlit app
│
├── models/
│   ├── lgbm_YYYYMMDD_HHMMSS.pkl
│   └── latest.txt               # Pointer to current best model
│
├── data/
│   └── init_schema.sql             # PostgreSQL database schema
│
├── tests/
│   ├── test_validation.py       # Unit tests for DAG 1 validation logic
│   ├── test_features.py         # Unit tests for lag feature computation
│   └── test_model.py            # Assert MAPE < 20% on holdout
│
├── notebooks/
│   └── 01_eda.ipynb             # Exploratory analysis (for portfolio write-up)
│
├── requirements.txt
├── README.md
└── .env.example
```

---

## REQUIREMENTS.TXT

```
apache-airflow==2.8.1
pandas==2.1.0
numpy==1.26.0
scikit-learn==1.3.0
lightgbm==4.1.0
joblib==1.3.2
streamlit==1.28.0
plotly==5.17.0
sqlalchemy==2.0.0
psycopg2-binary==2.9.9
pytest==7.4.0
python-dotenv==1.0.0
```

---

## UNIT TESTS (Minimum Viable)

Write the following 3 tests in `/tests/`:

1. **`test_validation.py`** — Given a row with a null `zone_id`, assert the validation task rejects it and logs it to `validation_errors`
2. **`test_features.py`** — Given a known sequence of order counts for Zone Z1, assert `lag_1h` is computed correctly (no cross-zone leakage)
3. **`test_model.py`** — Load the trained model artifact, run predictions on the holdout set, assert MAPE < 20%

---

## README STRUCTURE

The README must tell the project story for a recruiter in under 3 minutes:

```
## Hyperlocal Demand Forecasting Pipeline

### Problem
Dark stores (Blinkit, Zepto) need hourly order forecasts per zone 
to pre-position inventory and delivery partners.

### Solution
An end-to-end Airflow-orchestrated pipeline: ingest → validate → 
feature engineer → train → predict → visualize.

### Architecture Diagram
[Insert simple ASCII or image diagram of the 4-DAG flow]

### Tech Stack
- Orchestration: Apache Airflow 2.8
- Modeling: LightGBM
- Storage: PostgreSQL
- Dashboard: Streamlit

### Key Results
- Forecast MAPE: ~X% (vs ~Y% naive baseline)
- Pipeline handles 5 zones × 24 hours × 180 days of data
- Automated retraining trigger when MAPE > 15%

### How to Run
[Step-by-step setup instructions]

### What I Learned
[3–4 lines on Airflow DAG design, failure handling, lag feature engineering]
```

---

## PORTFOLIO FRAMING NOTES

When presenting this project in interviews, emphasize these three decisions:

1. **DAG dependency design:** "I separated ingestion, feature engineering, training, and prediction into 4 separate DAGs with explicit dependencies — so if feature engineering fails, the model never runs on stale data."

2. **Retraining trigger:** "I added a MAPE threshold check — if model accuracy drops below 15%, the training DAG is automatically triggered. This mirrors how production ML systems handle data drift."

3. **Baseline comparison before promotion:** "The model artifact is only promoted to production if it beats the naive baseline. This prevents a bad retrain from replacing a working model."

---

*This prompt contains the complete specification. Build each component in order: simulate data → set up Airflow → build DAGs 1–4 → build Streamlit dashboard → write tests → write README.*
