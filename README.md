# Food Prices & Inflation Tracking Pipeline

This repository contains a data pipeline built with **Apache Airflow** to extract, transform and load (ETL) food price and inflation data for **Kenya**. It supports economic insights and policy analysis by providing structured historical data for visualization and analytics.



## Project Overview

This project focuses on two main ETL workflows:

### Food Prices ETL
- Extracts and loads commodity price data across Kenyan markets into a PostgreSQL database.

### Inflation ETL
- Fetches annual Consumer Price Index (CPI) data from the World Bank API and stores it in PostgreSQL.



## Project Folder Structure

```text
food_prices/
├── .env
├── .gitignore
├── etl_cpi_prices_dag.py         # CPI extraction script
├── etl_food_prices_dag.py        # Raw food prices ETL script
├── airflow/                      # Airflow DAGs and configuration
│   └── dags/
│       └── ...
├── requirements.txt              # Python dependencies
└── README.md                     # Project documentation (this file!)
```

---

## Tech Stack

- **Apache Airflow** – Orchestrating the ETL workflows  
- **PostgreSQL** – Data warehouse  
- **Python** – Primary scripting language  
- **World Bank API** – Source of inflation data  
- **Jupyter Notebook** – Exploratory data analysis

---

## Setup Instructions

Follow these steps to run the pipeline locally:

### 1. Clone the Repository

```bash
git clone https://github.com/YourUsername/food-prices-project.git
cd food_prices_project
```

### 2. Create and Activate a Virtual Environment
```bash
python -m venv venv
source venv/bin/activate     # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Set Environment Variables

Create a .env file in the root directory with your PostgreSQL credentials:
```bash
PG_HOST=your_host
PG_PORT=your_port
PG_DATABASE=your_db
PG_USER=your_user
PG_PASSWORD=your_password
```
Replace placeholders with your actual PostgreSQL connection details.

### 5. Initialize and Start Airflow
```bash
export AIRFLOW_HOME=$(pwd)/airflow  # Set Airflow home directory

airflow db init

airflow users create \
    --username admin \
    --firstname John \
    --lastname Doe \
    --role Admin \
    --email admin@example.com \
    --password admin_password

airflow webserver --port 8080
airflow scheduler
```


### License

Licensed under the MIT License.
Feel free to fork, modify and reuse for your own projects!

