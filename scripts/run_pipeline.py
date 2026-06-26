import os
import sys
from datetime import datetime

# Add project root and dags directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), "dags"))

from dags.ingest_and_validate_dag import (
    check_new_data_fn, validate_schema_fn, validate_ranges_fn, write_clean_data_fn
)
from dags.feature_engineering_dag import (
    aggregate_hourly_fn, engineer_time_features_fn, engineer_lag_features_fn, write_features_fn
)
from dags.model_training_dag import (
    check_retrain_trigger_fn, load_training_data_fn, train_model_fn, evaluate_model_fn,
    compare_with_baseline_fn, save_model_artifact_fn
)
from dags.generate_predictions_dag import (
    load_latest_model_fn, generate_forecasts_fn, flag_high_demand_zones_fn
)

class MockTaskInstance:
    """Mocks Airflow's TaskInstance (ti) for XCom operations."""
    def __init__(self):
        self.store = {}
        
    def xcom_push(self, key, value):
        # Store by key for easy extraction
        self.store[key] = value
        
    def xcom_pull(self, task_ids=None, key=None):
        return self.store.get(key)

def run_full_pipeline():
    print("=" * 60)
    print("RUNNING END-TO-END PIPELINE VIA PYTHON MOCK ORCHESTRATOR")
    print("=" * 60)
    
    # ------------------ DAG 1: ingest_and_validate ------------------
    print("\n--- Running Ingest and Validate DAG ---")
    ti1 = MockTaskInstance()
    context1 = {'ti': ti1}
    
    check_res = check_new_data_fn(**context1)
    print(f"Task 1 (check_new_data): {check_res}")
    
    schema_res = validate_schema_fn(**context1)
    print(f"Task 2 (validate_schema): {schema_res}")
    
    range_res = validate_ranges_fn(**context1)
    print(f"Task 3 (validate_ranges): {range_res}")
    
    write_res = write_clean_data_fn(**context1)
    print(f"Task 4 (write_clean_data): {write_res}")
    
    # ------------------ DAG 2: feature_engineering ------------------
    print("\n--- Running Feature Engineering DAG ---")
    ti2 = MockTaskInstance()
    context2 = {'ti': ti2}
    
    agg_res = aggregate_hourly_fn(**context2)
    print(f"Task 1 (aggregate_hourly): {agg_res}")
    
    time_res = engineer_time_features_fn(**context2)
    print(f"Task 2 (engineer_time_features): {time_res}")
    
    lag_res = engineer_lag_features_fn(**context2)
    print(f"Task 3 (engineer_lag_features): {lag_res}")
    
    write_feat_res = write_features_fn(**context2)
    print(f"Task 4 (write_features): {write_feat_res}")
    
    # ------------------ DAG 3: model_training ------------------
    print("\n--- Running Model Training DAG ---")
    ti3 = MockTaskInstance()
    context3 = {'ti': ti3}
    
    try:
        trigger_res = check_retrain_trigger_fn(**context3)
        print(f"Task 1 (check_retrain_trigger): {trigger_res}")
        
        load_train_res = load_training_data_fn(**context3)
        print(f"Task 2 (load_training_data): {load_train_res}")
        
        train_res = train_model_fn(**context3)
        print(f"Task 3 (train_model): {train_res}")
        
        eval_res = evaluate_model_fn(**context3)
        print(f"Task 4 (evaluate_model): {eval_res}")
        
        comp_res = compare_with_baseline_fn(**context3)
        print(f"Task 5 (compare_with_baseline): {comp_res}")
        
        save_res = save_model_artifact_fn(**context3)
        print(f"Task 6 (save_model_artifact): {save_res}")
    except Exception as e:
        print(f"Model Training encountered an error/skip: {e}")
        
    # ------------------ DAG 4: generate_predictions ------------------
    print("\n--- Running Generate Predictions DAG ---")
    ti4 = MockTaskInstance()
    context4 = {'ti': ti4}
    
    load_model_res = load_latest_model_fn(**context4)
    print(f"Task 1 (load_latest_model): {load_model_res}")
    
    forecast_res = generate_forecasts_fn(**context4)
    print(f"Task 2 (generate_forecasts): {forecast_res}")
    
    alerts_res = flag_high_demand_zones_fn(**context4)
    print(f"Task 3 (flag_high_demand_zones): {alerts_res}")
    
    print("\n" + "=" * 60)
    print("PIPELINE EXECUTION COMPLETED SUCCESSFULLY!")
    print("=" * 60)

if __name__ == "__main__":
    run_full_pipeline()
