
import os
import io
import pandas as pd
import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load DB credentials
load_dotenv()
DB_URI = os.getenv("DB_URI")
if not DB_URI:
    raise ValueError("DB_URI is missing. Check your .env file.")


# 1. Fetch WFP data

def fetch_data():
    url = "https://data.humdata.org/dataset/e0d3fba6-f9a2-45d7-b949-140c455197ff/resource/517ee1bf-2437-4f8c-aa1b-cb9925b9d437/download/wfp_food_prices_ken.csv"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return pd.read_csv(io.StringIO(response.text))
    except requests.RequestException as e:
        raise Exception(f"Failed to fetch data: {e}")


# 2. Cleaning and normalising dta

def clean_data(df):
    df.columns = df.columns.str.lower().str.strip()
    df.rename(columns={'admin1': 'province', 'admin2': 'county'}, inplace=True)
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d', errors='coerce')

    # Convert numeric columns
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df['usdprice'] = pd.to_numeric(df['usdprice'], errors='coerce')
    df['commodity_id'] = pd.to_numeric(df['commodity_id'], errors='coerce').astype('Int64')
    df['market_id'] = pd.to_numeric(df['market_id'], errors='coerce').astype('Int64')

    # Drop rows missing key info
    df.dropna(subset=['price', 'usdprice', 'date', 'commodity_id', 'market_id'], inplace=True)
    return df


# 3. Building a star schema

def build_star_schema(df):
    
    commodity_dim = df[['commodity_id', 'commodity', 'category']].drop_duplicates()
    market_dim = df[['market_id', 'market', 'province', 'county', 'latitude', 'longitude']].drop_duplicates()

    date_dim = df[['date']].drop_duplicates().copy()
    date_dim['date_key'] = date_dim['date'].dt.strftime('%Y%m%d').astype(int)
    date_dim['year'] = date_dim['date'].dt.year
    date_dim['month'] = date_dim['date'].dt.month
    date_dim['day'] = date_dim['date'].dt.day

    df['date_key'] = df['date'].dt.strftime('%Y%m%d').astype(int)

    fact_food_prices = df[[
        'date_key', 'commodity_id', 'market_id', 'unit', 'price', 'usdprice',
        'pricetype', 'priceflag', 'currency'
    ]]

    return commodity_dim, market_dim, date_dim, fact_food_prices


# 4. Converting numpy types to Python

def convert_row(row):
    return [x.item() if hasattr(x, 'item') else str(x) if pd.isna(x) else x for x in row]


# 5. Loading using psycopg2

def load_to_postgres(tables):
    table_names = ["commodity_dim", "market_dim", "date_dim", "fact_food_prices"]
    conn = psycopg2.connect(DB_URI)
    cur = conn.cursor()

    # Create schema
    cur.execute("CREATE SCHEMA IF NOT EXISTS capstone;")
    conn.commit()

    for name, df in zip(table_names, tables):
        # Drop and recreate each table
        cur.execute(f"DROP TABLE IF EXISTS capstone.{name} CASCADE;")
        col_defs = ", ".join([f"{col} TEXT" for col in df.columns])
        cur.execute(f"CREATE TABLE capstone.{name} ({col_defs});")
        conn.commit()

        rows = [convert_row(r) for r in df.to_numpy().tolist()]
        insert_sql = f"INSERT INTO capstone.{name} ({', '.join(df.columns)}) VALUES %s"
        execute_values(cur, insert_sql, rows)
        conn.commit()
        print(f"Loaded {len(rows)} rows into capstone.{name}")

    cur.close()
    conn.close()


# 6. Running ETL

def main():
    print("Fetching data...")
    raw = fetch_data()

    print("🧹 Cleaning data...")
    clean = clean_data(raw)

    print("Building star schema...")
    tables = build_star_schema(clean)

    print("Inserting into PostgreSQL...")
    load_to_postgres(tables)

    print("ETL complete!")

if __name__ == "__main__":
    main()