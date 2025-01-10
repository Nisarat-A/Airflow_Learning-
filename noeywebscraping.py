from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import logging

# Configure logging
logger = logging.getLogger(__name__)

# Create table SQL
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS cryptocurrency (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50),
    price_usd DECIMAL(18,8),
    price_thb DECIMAL(18,8),
    market_cap DECIMAL(18,2),
    volume DECIMAL(18,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

def scrape_crypto_data(ti):
    """Scrape cryptocurrency data from CoinGecko"""
    try:
        url = "https://www.coingecko.com/en"
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        data = []
        table = soup.find('table', {'class': 'table-scrollable'})
        rows = table.find('tbody').find_all('tr')[:10]
        
        for row in rows:
            cols = row.find_all('td')
            name = cols[2].get_text(strip=True).split('\n')[0]
            price_usd = float(cols[3].get_text(strip=True).replace('$', '').replace(',', ''))
            market_cap = float(cols[6].get_text(strip=True).replace('$', '').replace(',', ''))
            volume = float(cols[7].get_text(strip=True).replace('$', '').replace(',', ''))
            data.append((name, price_usd, market_cap, volume))
        
        ti.xcom_push(key='crypto_data', value=data)
        logger.info(f"Scraped {len(data)} cryptocurrencies")
        
    except Exception as e:
        logger.error(f"Error scraping data: {e}")
        raise

def fetch_exchange_rate(ti):
    """Fetch USD to THB exchange rate"""
    try:
        url = "https://api.exchangerate-api.com/v4/latest/USD"
        response = requests.get(url, timeout=10)
        rate = response.json()['rates']['THB']
        
        ti.xcom_push(key='exchange_rate', value=rate)
        logger.info(f"Fetched exchange rate: 1 USD = {rate} THB")
        
    except Exception as e:
        logger.error(f"Error fetching exchange rate: {e}")
        raise

def transform_and_insert(ti):
    """Transform data and insert into database"""
    try:
        # Get data from XCom
        crypto_data = ti.xcom_pull(key='crypto_data', task_ids='scrape_crypto')
        exchange_rate = ti.xcom_pull(key='exchange_rate', task_ids='fetch_exchange')
        
        # Transform data
        transformed_data = [
            (
                name,
                price_usd,
                price_usd * exchange_rate,  # Convert to THB
                market_cap,
                volume
            )
            for name, price_usd, market_cap, volume in crypto_data
        ]
        
        # Insert into database
        postgres = PostgresHook(postgres_conn_id='postgres_default')
        insert_sql = """
        INSERT INTO cryptocurrency (name, price_usd, price_thb, market_cap, volume)
        VALUES (%s, %s, %s, %s, %s)
        """
        postgres.run(insert_sql, parameters=transformed_data)
        
        logger.info(f"Inserted {len(transformed_data)} rows into database")
        
    except Exception as e:
        logger.error(f"Error in transform and insert: {e}")
        raise

def load_data():
    """Load and verify data"""
    try:
        postgres = PostgresHook(postgres_conn_id='postgres_default')
        
        # Get latest data count
        result = postgres.get_first("SELECT COUNT(*) FROM cryptocurrency")
        total_rows = result[0]
        
        # Get latest prices
        latest = postgres.get_records("""
            SELECT name, price_usd, price_thb 
            FROM cryptocurrency 
            WHERE created_at >= NOW() - INTERVAL '1 day'
            ORDER BY created_at DESC 
            LIMIT 5
        """)
        
        logger.info(f"Total rows in database: {total_rows}")
        logger.info("Latest prices loaded successfully")
        
    except Exception as e:
        logger.error(f"Error in load step: {e}")
        raise

# DAG definition
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'simple_crypto_etl',
    default_args=default_args,
    description='Simple cryptocurrency ETL pipeline',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2025, 1, 1),
    catchup=False
)

# Create tasks
create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_default',
    sql=CREATE_TABLE_SQL,
    dag=dag
)

scrape_crypto = PythonOperator(
    task_id='scrape_crypto',
    python_callable=scrape_crypto_data,
    dag=dag
)

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

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag
)

# Set dependencies exactly as requested
create_table >> [scrape_crypto, fetch_exchange] >> transform_insert >> load_data_task