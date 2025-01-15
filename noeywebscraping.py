from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email import EmailOperator
from airflow.models import TaskInstance
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path
import logging
import json

# Configure logging
logger = logging.getLogger(__name__)

# SQL for table creation
CREATE_GOLD_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS gold_price (
    id SERIAL PRIMARY KEY,
    price_usd DECIMAL(18,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_CRYPTO_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS crypto_price (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10),
    price_usd DECIMAL(18,8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

def scrape_gold_price(ti):
    """Fetch gold price in USD from GoldAPI."""
    url = 'https://www.goldapi.io/api/XAU/USD'
    headers = {
        'x-access-token': 'goldapi-c0ax6vsm5w6jqxb-io',
        'Content-Type': 'application/json'
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        gold_price_usd = data['price']
        ti.xcom_push(key='gold_price', value=gold_price_usd)
        logging.info(f"Scraped Gold price: ${gold_price_usd:.2f}")
    except Exception as e:
        logging.error(f"Error fetching Gold price: {e}")
        raise

def scrape_crypto_prices(ti):
    """Fetch cryptocurrency prices in USD."""
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin,ethereum",
        "vs_currencies": "usd",
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        prices = response.json()

        # Extract prices only
        crypto_prices = {symbol: data['usd'] for symbol, data in prices.items()}
        ti.xcom_push(key='crypto_prices', value=crypto_prices)
        logger.info(f"Scraped cryptocurrency prices: {crypto_prices}")
    except Exception as e:
        logger.error(f"Error fetching cryptocurrency prices: {e}")
        raise
def list_all_cryptocurrencies(ti):
    """
    Fetch and list all available cryptocurrencies from the CoinGecko API.
    """
    try:
        url = "https://api.coingecko.com/api/v3/coins/list"
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        # Parse and log the cryptocurrencies
        cryptocurrencies = response.json()
        logger.info(f"Fetched {len(cryptocurrencies)} cryptocurrencies from the API.")

        # Push the list to XCom for downstream tasks
        ti.xcom_push(key='cryptocurrencies', value=cryptocurrencies)
    except Exception as e:
        logger.error(f"Error fetching cryptocurrencies: {e}")
        raise


def transform_to_thai_baht(ti):
    """Convert USD prices to THB."""
    url = "https://api.exchangerate-api.com/v4/latest/USD"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        thb_rate = response.json().get('rates', {}).get('THB', None)

        if thb_rate is None:
            raise ValueError("THB exchange rate not found in response.")

        gold_price = ti.xcom_pull(key='gold_price', task_ids='scrape_gold')
        crypto_prices = ti.xcom_pull(key='crypto_prices', task_ids='scrape_crypto')

        if gold_price is None or crypto_prices is None:
            raise ValueError("Missing gold or crypto price data from XCom.")

        gold_thb = gold_price * thb_rate
        crypto_thb = {symbol: {'thb': price * thb_rate, 'usd': price} for symbol, price in crypto_prices.items()}

        ti.xcom_push(key='gold_thb', value=gold_thb)
        ti.xcom_push(key='crypto_thb', value=crypto_thb)
        logger.info(f"Transformed prices to THB with rate: {thb_rate:.2f}")
    except Exception as e:
        logger.error(f"Error in THB transformation: {e}")
        raise

def insert_gold_price(ti):
    """Insert gold price into database."""
    try:
        price_usd = ti.xcom_pull(key='gold_price', task_ids='scrape_gold')
        postgres = PostgresHook(postgres_conn_id='Noey_Interns')
        postgres.run("""
            INSERT INTO gold_price (price_usd)
            VALUES (%s)
        """, parameters=(price_usd,))
        logger.info(f"Inserted gold price: ${price_usd:.2f}")
    except Exception as e:
        logger.error(f"Error inserting gold price: {e}")
        raise

def insert_crypto_prices(ti):
    """Insert cryptocurrency prices into database."""
    try:
        crypto_prices = ti.xcom_pull(key='crypto_prices', task_ids='scrape_crypto')
        postgres = PostgresHook(postgres_conn_id='Noey_Interns')
        for symbol, price in crypto_prices.items():
            postgres.run("""
                INSERT INTO crypto_price (symbol, price_usd)
                VALUES (%s, %s)
            """, parameters=(symbol, price))
        logger.info("Inserted crypto prices successfully")
    except Exception as e:
        logger.error(f"Error inserting crypto prices: {e}")
        raise

def generate_report(ti):
    """Generate daily price report."""
    try:
        report_dir = Path('/opt/airflow/dags/price_reports')
        report_dir.mkdir(exist_ok=True)
        today = datetime.now().strftime('%Y-%m-%d')
        report_path = report_dir / f'daily_price_report_{today}.html'

        postgres = PostgresHook(postgres_conn_id='Noey_Interns')

        gold_df = postgres.get_pandas_df("""
            SELECT price_usd, created_at
            FROM gold_price
            WHERE DATE(created_at) = CURRENT_DATE
            ORDER BY created_at DESC;
        """)

        crypto_df = postgres.get_pandas_df("""
            SELECT symbol, price_usd, created_at
            FROM crypto_price
            WHERE DATE(created_at) = CURRENT_DATE
            ORDER BY created_at DESC;
        """)

        html_content = f"""
        <html>
            <head>
                <style>
                    table {{ border-collapse: collapse; width: 100%; }}
                    th, td {{ border: 1px solid black; padding: 8px; text-align: left; }}
                    th {{ background-color: #f2f2f2; }}
                </style>
            </head>
            <body>
                <h1>Daily Price Report - {today}</h1>
                <h2>Gold Prices</h2>
                {gold_df.to_html(index=False)}
                <h2>Cryptocurrency Prices</h2>
                {crypto_df.to_html(index=False)}
            </body>
        </html>
        """

        with open(report_path, 'w') as f:
            f.write(html_content)

        ti.xcom_push(key='report_path', value=str(report_path))
        logger.info(f"Generated report at {report_path}")
    except Exception as e:
        logger.error(f"Error generating report: {e}")
        raise

# DAG Definition
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': ['wls.nagiosnotification@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
}

dag = DAG(
    'gold_crypto_pipeline',
    default_args=default_args,
    description='ETL pipeline for Gold and Cryptocurrency prices',
    schedule_interval='@hourly',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['gold', 'crypto', 'etl']
)

# Task Definitions
scrape_gold_task = PythonOperator(
    task_id='scrape_gold',
    python_callable=scrape_gold_price,
    dag=dag
)

scrape_crypto_task = PythonOperator(
    task_id='scrape_crypto',
    python_callable=scrape_crypto_prices,
    dag=dag
)

create_gold_table_task = PostgresOperator(
    task_id='create_gold_table',
    postgres_conn_id='Noey_Interns',
    sql=CREATE_GOLD_TABLE_SQL,
    dag=dag
)

create_crypto_table_task = PostgresOperator(
    task_id='create_crypto_table',
    postgres_conn_id='Noey_Interns',
    sql=CREATE_CRYPTO_TABLE_SQL,
    dag=dag
)

insert_gold_task = PythonOperator(
    task_id='insert_gold',
    python_callable=insert_gold_price,
    dag=dag
)

insert_crypto_task = PythonOperator(
    task_id='insert_crypto',
    python_callable=insert_crypto_prices,
    dag=dag
)

transform_to_thai_baht_task = PythonOperator(
    task_id='transform_thai_baht',
    python_callable=transform_to_thai_baht,
    dag=dag
)

make_report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag
)
send_email_task = EmailOperator(
    task_id='send_email',
    to=['noansrnn@gmail.com'],
    subject='Daily Price Report - {{ ds }}',
    html_content="""
    Daily price report for {{ ds }} is attached.<br/>
    <br/>
    Best regards,<br/>
    Airflow Pipeline
    """,
    files=['{{ ti.xcom_pull(task_ids="generate_report", key="report_path") }}'],
   conn_id='noey_email',  
    dag=dag
)
list_all_cryptos_task = PythonOperator(
    task_id='list_all_cryptos',
    python_callable=list_all_cryptocurrencies,
    dag=dag
)

# Set up task dependencies
(
    [scrape_crypto_task >> create_crypto_table_task >> insert_crypto_task] +
    [scrape_gold_task >> create_gold_table_task >> insert_gold_task]
) >> transform_to_thai_baht_task >> make_report_task >> send_email_task