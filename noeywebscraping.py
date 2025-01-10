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
logger = logging.getLogger(__name__)

# SQL for table creation - simplified for Bitcoin only
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS bitcoin_price (
    id SERIAL PRIMARY KEY,
    price_usd DECIMAL(18,8),
    price_thb DECIMAL(18,8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

def scrape_bitcoin_price(ti):
    """Scrape Bitcoin price from Google"""
    try:
        url = 'https://www.google.com/search?q=bitcoin+price+dollar'
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        price_element = soup.find('span', class_="pclqee")
        if not price_element:
            raise ValueError("Bitcoin price element not found on the page")
            
        price_str = price_element.text.replace('$', '').replace(',', '')
        price_usd = float(price_str)
        
        ti.xcom_push(key='btc_price', value=price_usd)
        logger.info(f"Successfully scraped Bitcoin price: ${price_usd:,.2f}")
        
    except requests.exceptions.RequestException as req_err:
        logger.error(f"HTTP request error: {req_err}")
        raise
    except Exception as e:
        logger.error(f"Error scraping Bitcoin price: {e}")
        raise

def fetch_exchange_rate(ti):
    """Fetch USD to THB exchange rate"""
    try:
        url = "https://api.exchangerate-api.com/v4/latest/USD"
        response = requests.get(url, timeout=10)
        rate = response.json()['rates']['THB']
        
        ti.xcom_push(key='exchange_rate', value=rate)
        logger.info(f"Exchange rate fetched: 1 USD = {rate} THB")
        
    except Exception as e:
        logger.error(f"Error fetching exchange rate: {e}")
        raise

def transform_and_insert(ti):
    """Transform and insert Bitcoin price data into database"""
    try:
        btc_price_usd = ti.xcom_pull(key='btc_price', task_ids='scrape_bitcoin')
        exchange_rate = ti.xcom_pull(key='exchange_rate', task_ids='fetch_exchange')
        
        btc_price_thb = btc_price_usd * exchange_rate
        
        postgres = PostgresHook(postgres_conn_id='Noey_Interns')
        insert_sql = """
        INSERT INTO bitcoin_price (price_usd, price_thb)
        VALUES (%s, %s)
        """
        postgres.run(insert_sql, parameters=(btc_price_usd, btc_price_thb))
        logger.info(f"Inserted Bitcoin price: ${btc_price_usd:,.2f} (฿{btc_price_thb:,.2f})")
        
    except Exception as e:
        logger.error(f"Error in transform and insert: {e}")
        raise

def export_data():
    """Export Bitcoin price data to multiple formats and summarize trends."""
    try:
        # Create export directory in the DAGs folder
        export_dir = Path('/opt/airflow/dags/bitcoin_reports')
        export_dir.mkdir(exist_ok=True)

        today = datetime.now().strftime('%Y-%m-%d')

        # Get data from database
        postgres = PostgresHook(postgres_conn_id='Noey_Interns')
        query = """
        SELECT 
            price_usd,
            price_thb,
            created_at
        FROM bitcoin_price 
        WHERE DATE(created_at) = CURRENT_DATE
        ORDER BY created_at DESC;
        """

        df = postgres.get_pandas_df(query)

        # 1. Export to CSV
        csv_path = export_dir / f'bitcoin_prices_{today}.csv'
        df.to_csv(csv_path, index=False)

        # 2. Export to Excel with summary
        excel_path = export_dir / f'bitcoin_analysis_{today}.xlsx'
        with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
            # Main data sheet
            df.to_excel(writer, sheet_name='Prices', index=False)

            # Summary sheet
            summary = pd.DataFrame({
                'Metric': [
                    'Latest Price (USD)',
                    'Latest Price (THB)',
                    'Daily High (USD)',
                    'Daily Low (USD)',
                    'Price Change (%)',
                    'Date Generated'
                ],
                'Value': [
                    f"${df['price_usd'].iloc[0]:,.2f}",
                    f"฿{df['price_thb'].iloc[0]:,.2f}",
                    f"${df['price_usd'].max():,.2f}",
                    f"${df['price_usd'].min():,.2f}",
                    f"{((df['price_usd'].iloc[0] - df['price_usd'].iloc[-1]) / df['price_usd'].iloc[-1] * 100):,.2f}%",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ]
            })
            summary.to_excel(writer, sheet_name='Summary', index=False)

        # 3. Generate Text-Based Price History
        trend_report = "Bitcoin Price History (Last 24 Hours):\n\n"
        for _, row in df.iterrows():
            trend_report += f"{row['created_at']}: ${row['price_usd']:,.2f} (฿{row['price_thb']:,.2f})\n"

        # Save trend analysis
        trend_path = export_dir / f'bitcoin_trend_{today}.txt'
        with open(trend_path, 'w') as file:
            file.write(trend_report)

        logger.info(f"""
        Export completed successfully:
        - CSV: {csv_path}
        - Excel: {excel_path}
        - Trend Analysis: {trend_path}
        """)

    except Exception as e:
        logger.error(f"Error in data export: {e}")
        raise

# DAG definition
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bitcoin_price_pipeline',
    default_args=default_args,
    description='ETL pipeline for Bitcoin price data from Google',
    schedule_interval=timedelta(minutes=30),  # Run every 30 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['bitcoin', 'etl']
)

# Create tasks
create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='Noey_Interns',
    sql=CREATE_TABLE_SQL,
    dag=dag
)

scrape_bitcoin = PythonOperator(
    task_id='scrape_bitcoin',
    python_callable=scrape_bitcoin_price,
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

export_reports = PythonOperator(
    task_id='export_reports',
    python_callable=export_data,
    dag=dag
)

# Define task dependencies
create_table >> [scrape_bitcoin, fetch_exchange] >> transform_insert >> export_reports