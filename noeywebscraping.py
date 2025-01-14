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
    price_thb DECIMAL(18,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_BITCOIN_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS crypto_price (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10),
    price_usd DECIMAL(18,8),
    price_thb DECIMAL(18,8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""
def scrape_gold_price(ti):
    """Scrape gold price in USD from the specified Gold.org website."""
    url = 'https://www.gold.org/goldhub/data#price-and-premium'
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36"
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Locate the <p class="value"> element for gold price
        gold_price_element = soup.find('p', class_='value')
        if not gold_price_element:
            raise ValueError("Gold price element not found")

        # Extract the gold price
        gold_price_usd = float(gold_price_element.text.strip().replace(',', ''))

        # Push the price to XCom
        ti.xcom_push(key='gold_price', value=gold_price_usd)

        logger.info(f"Scraped Gold price: ${gold_price_usd:.2f}")
    except Exception as e:
        logger.error(f"Error scraping Gold price: {e}")
        raise


def scrape_crypto_prices(ti):
    """Fetch cryptocurrency prices in USD."""
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin,ethereum",
        "vs_currencies": "usd",
        "include_market_cap": "true",
        "include_24hr_vol": "true",
        "include_24hr_change": "true",
        "include_last_updated_at": "true"
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


def transform_to_thai_baht(ti):
    """Convert USD prices to THB."""
    url = "https://api.exchangerate-api.com/v4/latest/USD"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        thb_rate = response.json()['rates']['THB']
        
        # Get prices from XCom
        gold_price = ti.xcom_pull(key='gold_price', task_ids='scrape_gold')
        crypto_prices = ti.xcom_pull(key='crypto_prices', task_ids='scrape_crypto')
        
        # Transform prices
        gold_thb = gold_price * thb_rate
        crypto_thb = {
            symbol: {'thb': price['usd'] * thb_rate, 'usd': price['usd']}
            for symbol, price in crypto_prices.items()
        }
        
        # Push transformed prices
        ti.xcom_push(key='gold_thb', value=gold_thb)
        ti.xcom_push(key='crypto_thb', value=crypto_thb)
        logger.info(f"Transformed prices to THB with rate: {thb_rate}")
    except Exception as e:
        logger.error(f"Error in THB transformation: {e}")
        raise

def insert_gold_price(ti):
    """Insert gold price into database."""
    try:
        price_usd = ti.xcom_pull(key='gold_price', task_ids='scrape_gold')
        price_thb = ti.xcom_pull(key='gold_thb', task_ids='transform_to_thai_baht')
        
        postgres = PostgresHook(postgres_conn_id='Noey_Interns')
        postgres.run(""" 
            INSERT INTO gold_price (price_usd, price_thb) 
            VALUES (%s, %s) 
        """, parameters=(price_usd, price_thb))
        logger.info(f"Inserted gold price: ${price_usd:,.2f} (฿{price_thb:,.2f})")
    except Exception as e:
        logger.error(f"Error inserting gold price: {e}")
        raise

def insert_crypto_prices(ti):
    """Insert cryptocurrency prices into database."""
    try:
        crypto_prices = ti.xcom_pull(key='crypto_thb', task_ids='transform_to_thai_baht')
        postgres = PostgresHook(postgres_conn_id='Noey_Interns')
        
        for symbol, prices in crypto_prices.items():
            postgres.run("""
                INSERT INTO crypto_price (symbol, price_usd, price_thb)
                VALUES (%s, %s, %s)
            """, parameters=(symbol, prices['usd'], prices['thb']))
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
        
        # Fetch today's data
        gold_df = postgres.get_pandas_df("""
            SELECT price_usd, price_thb, created_at
            FROM gold_price
            WHERE DATE(created_at) = CURRENT_DATE
            ORDER BY created_at DESC;
        """)
        
        crypto_df = postgres.get_pandas_df("""
            SELECT symbol, price_usd, price_thb, created_at
            FROM crypto_price
            WHERE DATE(created_at) = CURRENT_DATE
            ORDER BY created_at DESC;
        """)
        
        # Generate HTML report
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

def send_success_email(context):
    """Send success email with task execution details."""
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']
    
    success_content = f"""
    <h2>ETL Pipeline Completed Successfully</h2>
    <p>DAG: {dag_id}</p>
    <p>Execution Date: {execution_date}</p>
    <p>Task Details:</p>
    <ul>
        <li>Gold Price Scraping: {task_instance.xcom_pull(task_ids='scrape_gold')}</li>
        <li>Crypto Price Scraping: {task_instance.xcom_pull(task_ids='scrape_crypto')}</li>
    </ul>
    <p>Please find the daily price report attached.</p>
    """
    
    return EmailOperator(
        task_id='send_success_email',
        to=['noansrnn@gmail.com'],
        subject=f'Success: Daily Price Report {execution_date}',
        html_content=success_content,
        files=[context['task_instance'].xcom_pull(task_ids='make_report', key='report_path')]
    ).execute(context=context)

def send_retry_email(context):
    """Send retry notification email."""
    task_instance: TaskInstance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = task_instance.task_id
    try_number = task_instance.try_number
    max_tries = task_instance.max_tries
    execution_date = context['execution_date']
    
    retry_content = f"""
    <h2>Task Retry Notification</h2>
    <p>DAG: {dag_id}</p>
    <p>Task: {task_id}</p>
    <p>Execution Date: {execution_date}</p>
    <p>Attempt: {try_number} of {max_tries}</p>
    <p>Error Message: {context.get('exception', 'Unknown error')}</p>
    """
    
    return EmailOperator(
        task_id='send_retry_email',
        to=['noansrnn@gmail.com'],
        subject=f'Retry: ETL Pipeline Task {task_id}',
        html_content=retry_content
    ).execute(context=context)

def send_failure_email(context):
    """Send failure notification email."""
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = task_instance.task_id
    execution_date = context['execution_date']
    
    failure_content = f"""
    <h2>ETL Pipeline Task Failed</h2>
    <p>DAG: {dag_id}</p>
    <p>Task: {task_id}</p>
    <p>Execution Date: {execution_date}</p>
    <p>Error Message: {context.get('exception', 'Unknown error')}</p>
    <p>Log URL: {task_instance.log_url}</p>
    """
    
    return EmailOperator(
        task_id='send_failure_email',
        to=['noansrnn@gmail.com'],
        subject=f'Failed: ETL Pipeline Task {task_id}',
        html_content=failure_content
    ).execute(context=context)

# DAG Definition
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': ['noansrnn@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'on_failure_callback': send_failure_email,
    'on_retry_callback': send_retry_email,
    'on_success_callback': send_success_email
}

dag = DAG(
    'gold_crypto_pipeline',
    default_args=default_args,
    description='ETL pipeline for Gold and Cryptocurrency prices',
    schedule_interval='0 * * * *',  # Runs at minute 0 of every hour
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['gold', 'crypto', 'etl']
)

# Task Definition
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

transform_to_thai_baht_task = PythonOperator(
    task_id='transform_to_thai_baht',
    python_callable=transform_to_thai_baht,
    dag=dag
)

create_gold_table_task = PostgresOperator(
    task_id='create_gold_table',
    postgres_conn_id='Noey_Interns',
    sql=CREATE_GOLD_TABLE_SQL,
    dag=dag
)

create_bitcoin_table_task = PostgresOperator(
    task_id='create_bitcoin_table',
    postgres_conn_id='Noey_Interns',
    sql=CREATE_BITCOIN_TABLE_SQL,
    dag=dag
)

insert_gold_task = PythonOperator(
    task_id='insert_gold',
    python_callable=insert_gold_price,
    dag=dag
)

insert_bitcoin_task = PythonOperator(
    task_id='insert_bitcoin',
    python_callable=insert_crypto_prices,
    dag=dag
)

make_report_task = PythonOperator(
    task_id='make_report',
    python_callable=generate_report,
    dag=dag
)

send_email_task = EmailOperator(
    task_id='send_email',
    to=['noansrnn@gmail.com'],
    subject='Daily Price Report {{ ds }}',
    html_content='Please find attached the daily price report.',
    files=['{{ ti.xcom_pull(task_ids="make_report", key="report_path") }}'],
    dag=dag
)

# Define task dependencies
[scrape_gold_task >> transform_to_thai_baht_task, scrape_crypto_task >> transform_to_thai_baht_task]
transform_to_thai_baht_task >> [create_gold_table_task >> insert_gold_task, create_bitcoin_table_task >> insert_bitcoin_task]
[insert_gold_task, insert_bitcoin_task] >> make_report_task >> send_email_task
