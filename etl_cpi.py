
import os
import requests
import pandas as pd
import psycopg2
from dotenv import load_dotenv


load_dotenv()

def fetch_cpi_data():
    url = "https://api.worldbank.org/v2/country/ke/indicator/FP.CPI.TOTL?format=json&date=2006:2024"
    response = requests.get(url)
    response.raise_for_status()

    data = response.json()[1]
    return pd.DataFrame([
        {
            "year": int(row["date"]),
            "country": row["country"]["id"],
            "cpi": row["value"]
        }
        for row in data if row["value"] is not None
    ])

def insert_data_psycopg2(df):
    conn = psycopg2.connect(
        dbname=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT")
    )
    conn.autocommit = True
    cur = conn.cursor()

    # Ensuring schema exists
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

    # Insert rows with conflict resolution
    insert_query = """
        INSERT INTO capstone.fact_cpi (year, country, cpi)
        VALUES (%s, %s, %s)
        ON CONFLICT (year, country) DO UPDATE SET cpi = EXCLUDED.cpi;
    """
    cur.executemany(insert_query, df.values.tolist())

    conn.commit()
    cur.close()
    conn.close()
    print("Data inserted into capstone.fact_cpi")

if __name__ == "__main__":
    print("Fetching CPI data...")
    df = fetch_cpi_data()
    print(df.head())

    print("Inserting into PostgreSQL using psycopg2...")
    insert_data_psycopg2(df)
    print(" Done.")