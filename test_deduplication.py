from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

# Simple test function
def test_deduplicate_asins(**kwargs):
    # Simulate mock ASIN data
    mocked_response = [
        {"asin": "B01N5IB20Q"},
        {"asin": "B01N5IB20Q"},  # Duplicate
        {"asin": "B01N7FXW3P"},
        {"asin": "B01N8Q3D4Z"}
    ]

    # Extract unique ASINs
    unique_asins = list({item["asin"] for item in mocked_response if "asin" in item})
    print(f"Unique ASINs: {unique_asins}")
    return unique_asins

# DAG definition
default_args = {
    'owner': 'Daniel Fernandez',
    'start_date': datetime(2024, 11, 1),
    'retries': 1,
}

with DAG(
    'simple_test_amazon_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    test_task = PythonOperator(
        task_id='test_deduplicate_asins',
        python_callable=test_deduplicate_asins,
    )
