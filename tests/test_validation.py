import pytest
from unittest.mock import MagicMock
import os
import sys

# Add root folder to path for import
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from dags.ingest_and_validate_dag import validate_schema_fn, validate_ranges_fn

def test_validate_schema_rejects_null_zone_id():
    """Verify that validate_schema_fn flags records with null zone_id."""
    ti = MagicMock()
    # Dummy record with null zone_id
    ti.xcom_pull.return_value = [
        {
            "order_id": "test-order-1",
            "zone_id": None, # Null zone_id
            "timestamp": "2026-01-01 12:00:00",
            "order_value": 500.0,
            "items_count": 3,
            "delivery_time_mins": 15.0,
            "is_weekend": False,
            "weather_condition": "sunny",
            "is_festival_day": False
        }
    ]
    
    context = {"ti": ti}
    validate_schema_fn(**context)
    
    # Check that xcom_push was called with the rejected records
    ti.xcom_push.assert_called_once()
    call_args = ti.xcom_push.call_args_list[0][1]
    assert call_args['key'] == 'schema_invalid_records'
    
    invalid_records = call_args['value']
    assert "test-order-1" in invalid_records
    assert "Missing required field: zone_id" in invalid_records["test-order-1"]

def test_validate_ranges_rejects_out_of_bounds_values():
    """Verify that validate_ranges_fn flags negative values and out of bounds items count."""
    ti = MagicMock()
    ti.xcom_pull.return_value = [
        {
            "order_id": "test-order-2",
            "zone_id": "Z1",
            "timestamp": "2026-01-01 12:00:00",
            "order_value": -10.0,      # Invalid negative value
            "items_count": 25,         # Invalid items count (> 20)
            "delivery_time_mins": 15.0,
            "is_weekend": False,
            "weather_condition": "sunny",
            "is_festival_day": False
        }
    ]
    
    context = {"ti": ti}
    validate_ranges_fn(**context)
    
    # Check that range invalid records were correctly flagged
    ti.xcom_push.assert_called_once()
    call_args = ti.xcom_push.call_args_list[0][1]
    assert call_args['key'] == 'range_invalid_records'
    
    invalid_records = call_args['value']
    assert "test-order-2" in invalid_records
    assert "order_value is not positive" in invalid_records["test-order-2"]
    assert "items_count out of range" in invalid_records["test-order-2"]
