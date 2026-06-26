# Hyperlocal Demand Forecasting Pipeline — Dark Store Edition

A portfolio data engineering and machine learning project simulating a real-world ops forecasting problem solved by dark store quick-commerce companies like Blinkit, Zepto, and Swiggy Instamart.

##  Problem Statement
Dark stores (micro-warehouses) need hourly order forecasting per delivery zone to:
- Pre-pack high-velocity SKUs (instant dispatch).
- Strategically allocate delivery partners to zones ahead of peak demand.
- Trigger auto-replenishment from mother warehouses.

This project builds the end-to-end data pipeline that automates, orchestrates, trains, and monitors this forecasting process.

##  Pipeline Architecture
```
                         [Data Simulation Script] (scripts/simulate_data.py)
                                     ↓
                          PostgreSQL (raw_orders)
                                     ↓
[DAG 1: Ingest & Validate] ──(Reads new orders, runs schema & range checks)──> PostgreSQL (clean_orders)
                                     ↓
[DAG 2: Feature Engineering] ─(Hourly aggregation, lags, rolling stats)──────> PostgreSQL (features)
                                     ↓
[DAG 3: Model Training] ─────(Trains LightGBM, evaluates vs baseline)────────> Models Directory (.pkl)
                                     ↓ (Promoted model pointer)
[DAG 4: Generate Predictions] (Recursive multi-step forecasts)───────────────> PostgreSQL (predictions)
                                     ↓
[Streamlit Dashboard] ───────(Ops Heatmap, Model Monitor, DAG Health) <────── PostgreSQL
```

## Tech Stack
- **Orchestration:** Apache Airflow 2.10 (Local Sequenced Execution)
- **Database:** PostgreSQL 18
- **Modeling & ML:** LightGBM, Scikit-learn, Joblib
- **Data Manipulation:** Pandas, NumPy, SQLAlchemy, Psycopg2
- **Dashboard:** Streamlit, Plotly
- **Unit Testing:** Pytest

##  Key Results
- **Forecast MAPE:** ~8-12% (outperforms naive last-week baseline by ~15-20% absolute percentage error).
- **Scalability:** Pipeline processes ~200,000+ orders across 5 zones for 6 months in seconds.
- **Drift Protection:** Model automatically retrains if prediction MAPE on the last 24 hours exceeds 15%.
- **Promotion Safety:** Retrained model only replaces the active model if it beats the baseline.

##  How to Run

### 1. Setup Virtual Environment & Install Dependencies
Ensure you have Python 3.13 and PostgreSQL running.
```bash
# Create and activate virtual environment
python -m venv .venv
.venv\Scripts\activate

# Install package dependencies
pip install -r requirements.txt
```

### 2. Configure Environment Variables
Create a `.env` file in the root directory (based on `.env.example`).
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=hyperlocal_demand
DB_USER=postgres
DB_PASSWORD=your_postgres_password
AIRFLOW_HOME=./airflow
```

### 3. Initialize Database & Run Simulator
Initialize the database tables and populate 6 months of synthetic order transactions:
```bash
# Initialize schemas
python scripts/db_utils.py

# Seed raw_orders table with 6 months of historical transactions
python scripts/simulate_data.py
```

### 4. Execute the Airflow Pipeline
Since Airflow has platform limitations on native Windows (signals, gunicorn), we trigger the DAG tasks locally and sequentially via Airflow's testing CLI, which registers metadata and updates database states:
```bash
# Set Airflow environment home variable
$env:AIRFLOW_HOME = "$(pwd)/airflow"

# Initialize Airflow Metadata database
airflow db init

# Run Ingestion & Schema/Range Validation DAG
airflow dags test ingest_and_validate

# Run Feature Engineering DAG
airflow dags test feature_engineering

# Run Daily Model Training DAG (creates models/lgbm_*.pkl and latest.txt)
airflow dags test model_training

# Run Hourly Prediction & Alert Dispatch DAG
airflow dags test generate_predictions
```

### 5. Launch the Dashboard
Start the Streamlit operations dashboard to view predicted demand heatmaps, model performance logs, and DAG pipeline health:
```bash
streamlit run app/dashboard.py
```

### 6. Run Unit Tests
Execute the test suites:
```bash
pytest tests/
```

##  What I Learned
- **DAG Isolation:** Keeping Ingestion, Engineering, Training, and Prediction separated prevents one bottleneck from corrupting downstream states.
- **Time-Series Alignment:** Reindexing aggregated order counts to a complete hourly grid is essential. Without it, lag features shift incorrectly during periods of 0 orders.
- **Data Leakage Mitigation:** Aggregations and rolling features must be computed **per zone** and shifted properly. Using `shift(1)` on rolling averages ensures features only look at past intervals.
- **Model Promotion Safeguards:** Implementing baseline validation (comparing against last week's actuals) prevents a failed retrain from going live.
