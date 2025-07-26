from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import subprocess

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

# Defining DAG
with DAG(
    dag_id='etl_food_prices_dag',
    default_args=default_args,
    description='Daily ETL pipeline for food prices',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['capstone', 'food_prices'],
) as dag:

    def run_etl_script():
        script_path = os.path.join(os.path.dirname(__file__), 'etl_food_prices.py')
        result = subprocess.run(['python', script_path], capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Script failed:\n{result.stderr}")
        print(result.stdout)

    run_etl = PythonOperator(
        task_id='run_etl_food_prices',
        python_callable=run_etl_script,
    )

    run_etl