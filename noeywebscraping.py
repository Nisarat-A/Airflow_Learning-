from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
CREATE_TABLE_BITCOIN_SQL = """
CREATE TABLE IF NOT EXISTS bitcoin_price (
    id SERIAL PRIMARY KEY,
    price_usd DECIMAL(18,8),
    price_thb DECIMAL(18,8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_TABLE_GOLD_SQL = """
CREATE TABLE IF NOT EXISTS gold_price (
    id SERIAL PRIMARY KEY,
    price_usd DECIMAL(18,8),
    price_thb DECIMAL(18,8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

BITCOIN_PRICE_URL = 'https://www.google.com/search?q=bitcoin+price+dollar'
GOLD_PRICE_URL = 'https://www.google.com/search?q=gold+price+dollar'
EXCHANGE_RATE_API = "https://api.exchangerate-api.com/v4/latest/USD"
EXPORT_DIR = Path('/opt/airflow/dags/reports')

# Helper Functions
def scrape_price(url, ti_key, ti):
    """Scrape price in USD from Google search results."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        price_element = soup.find('span', class_="pclqee")
        if not price_element:
            raise ValueError(f"Price element not found on the page: {url}")
        price_usd = float(price_element.text.replace('$', '').replace(',', ''))
        ti.xcom_push(key=ti_key, value=price_usd)
        logger.info(f"Scraped price for {ti_key}: ${price_usd:,.2f}")
    except Exception as e:
        logger.error(f"Error scraping price for {ti_key}: {e}")
        raise

def fetch_exchange_rate(ti):
    """Fetch USD to THB exchange rate."""
    try:
        response = requests.get(EXCHANGE_RATE_API, timeout=10)
        response.raise_for_status()
        rate = response.json()['rates']['THB']
        ti.xcom_push(key='exchange_rate', value=rate)
        logger.info(f"Exchange rate fetched: 1 USD = {rate} THB")
    except Exception as e:
        logger.error(f"Error fetching exchange rate: {e}")
        raise

def transform_and_insert(ti):
    """Transform data and insert Bitcoin and Gold prices into the database."""
    try:
        btc_price_usd = ti.xcom_pull(key='btc_price', task_ids='scrape_bitcoin')
        gold_price_usd = ti.xcom_pull(key='gold_price', task_ids='scrape_gold')
        exchange_rate = ti.xcom_pull(key='exchange_rate', task_ids='fetch_exchange')

        btc_price_thb = btc_price_usd * exchange_rate
        gold_price_thb = gold_price_usd * exchange_rate

        postgres = PostgresHook(postgres_conn_id='Noey_Interns')

        postgres.run(
            """
            INSERT INTO bitcoin_price (price_usd, price_thb)
            VALUES (%s, %s)
            """,
            parameters=(btc_price_usd, btc_price_thb)
        )
        postgres.run(
            """
            INSERT INTO gold_price (price_usd, price_thb)
            VALUES (%s, %s)
            """,
            parameters=(gold_price_usd, gold_price_thb)
        )

        logger.info(f"Inserted Bitcoin price: ${btc_price_usd:,.2f} (฿{btc_price_thb:,.2f})")
        logger.info(f"Inserted Gold price: ${gold_price_usd:,.2f} (฿{gold_price_thb:,.2f})")
    except Exception as e:
        logger.error(f"Error in transform and insert: {e}")
        raise

def make_report():
    """Create reports for Bitcoin and Gold prices."""
    EXPORT_DIR.mkdir(exist_ok=True)
    today = datetime.now().strftime('%Y-%m-%d')
    try:
        postgres = PostgresHook(postgres_conn_id='Noey_Interns')

        for table in ['bitcoin_price', 'gold_price']:
            query = f"""
                SELECT price_usd, price_thb, created_at
                FROM {table}
                WHERE DATE(created_at) = CURRENT_DATE
                ORDER BY created_at DESC;
            """
            df = postgres.get_pandas_df(query)

            # Export to CSV
            csv_path = EXPORT_DIR / f'{table}_prices_{today}.csv'
            df.to_csv(csv_path, index=False)
            logger.info(f"Exported {table} report to CSV at {csv_path}")

    except Exception as e:
        logger.error(f"Error creating reports: {e}")
        raise

# DAG Definition
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    'price_pipeline',
    default_args=default_args,
    description='ETL pipeline for Bitcoin and Gold price data',
    schedule_interval=timedelta(minutes=30),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['bitcoin', 'gold', 'etl']
)

# Tasks
create_tables = [
    PostgresOperator(
        task_id=f'create_table_{name}',
        postgres_conn_id='Noey_Interns',
        sql=sql,
        dag=dag
    ) for name, sql in zip(['bitcoin', 'gold'], [CREATE_TABLE_BITCOIN_SQL, CREATE_TABLE_GOLD_SQL])
]

scrape_tasks = [
    PythonOperator(
        task_id=f'scrape_{name}',
        python_callable=scrape_price,
        op_kwargs={'url': url, 'ti_key': f'{name}_price'},
        dag=dag
    ) for name, url in zip(['bitcoin', 'gold'], [BITCOIN_PRICE_URL, GOLD_PRICE_URL])
]

fetch_exchange = PythonOperator(
    task_id='fetch_exchange',
    python_callable=fetch_exchange_rate,
    dag=dag
)

transform_insert = PythonOperator(
    task_id='transform_insert',
    python_callable=transform_and_insert,
    dag=dag
)

make_and_export_report = PythonOperator(
    task_id='make_and_export_report',
    python_callable=make_report,
    dag=dag
)

# Task Dependencies
create_tables >> scrape_tasks + [fetch_exchange] >> transform_insert >> make_and_export_report
