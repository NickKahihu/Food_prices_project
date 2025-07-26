from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import requests
import os
import psycopg2
from dotenv import load_dotenv


load_dotenv()


# These are the Core Functions

def fetch_cpi_data():
    url = "https://api.worldbank.org/v2/country/ke/indicator/FP.CPI.TOTL?format=json&date=2006:2024"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()[1]

    df = pd.DataFrame([
        {
            "year": int(row["date"]),
            "country": row["country"]["id"],
            "cpi": row["value"]
        }
        for row in data if row["value"] is not None
    ])

    df.to_csv("/tmp/cpi_data.csv", index=False)

def insert_data_psycopg2():
    df = pd.read_csv("/tmp/cpi_data.csv")

    conn = psycopg2.connect(
        dbname=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT")
    )
    cur = conn.cursor()

    cur.execute("CREATE SCHEMA IF NOT EXISTS capstone;")

    cur.execute("""
        DROP TABLE IF EXISTS capstone.fact_cpi;

        CREATE TABLE capstone.fact_cpi (
            year INT NOT NULL,
            country TEXT NOT NULL,
            cpi NUMERIC,
            PRIMARY KEY (year, country)
        );
    """)

    insert_query = """
        INSERT INTO capstone.fact_cpi (year, country, cpi)
        VALUES (%s, %s, %s)
        ON CONFLICT (year, country) DO UPDATE SET cpi = EXCLUDED.cpi;
    """
    cur.executemany(insert_query, df.values.tolist())
    conn.commit()
    cur.close()
    conn.close()


# Airflow DAG Definition

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}

with DAG(
    dag_id="inflation_data_etl",
    description="Fetch and insert Kenya CPI data from World Bank",
    schedule="@daily",  # You can also use None for manual runs
    default_args=default_args,
    catchup=False
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_cpi_data",
        python_callable=fetch_cpi_data
    )

    insert_task = PythonOperator(
        task_id="insert_cpi_data_to_postgres",
        python_callable=insert_data_psycopg2
    )

    fetch_task >> insert_task