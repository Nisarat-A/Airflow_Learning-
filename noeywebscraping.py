from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import pandas as pd
import plotly.express as px
from pathlib import Path
import logging

# Configure logging
logger = logging.getLogger(__name__)

# SQL for table creation
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
        rows = table.find('tbody').find_all('tr')[:10]  # Top 10 cryptocurrencies
        
        for row in rows:
            cols = row.find_all('td')
            name = cols[2].get_text(strip=True).split('\n')[0]
            price_usd = float(cols[3].get_text(strip=True).replace('$', '').replace(',', ''))
            market_cap = float(cols[6].get_text(strip=True).replace('$', '').replace(',', ''))
            volume = float(cols[7].get_text(strip=True).replace('$', '').replace(',', ''))
            data.append((name, price_usd, market_cap, volume))
        
        ti.xcom_push(key='crypto_data', value=data)
        logger.info(f"Successfully scraped {len(data)} cryptocurrencies")
        
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
        logger.info(f"Exchange rate fetched: 1 USD = {rate} THB")
        
    except Exception as e:
        logger.error(f"Error fetching exchange rate: {e}")
        raise

def transform_and_insert(ti):
    """Transform and insert data into database"""
    try:
        crypto_data = ti.xcom_pull(key='crypto_data', task_ids='scrape_crypto')
        exchange_rate = ti.xcom_pull(key='exchange_rate', task_ids='fetch_exchange')
        
        transformed_data = [
            (
                name,
                price_usd,
                price_usd * exchange_rate,
                market_cap,
                volume
            )
            for name, price_usd, market_cap, volume in crypto_data
        ]
        
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

def export_data():
    """Export cryptocurrency data to multiple formats"""
    try:
        # Create export directory in the DAGs folder
        export_dir = Path('/opt/airflow/dags/exports')
        export_dir.mkdir(exist_ok=True)
        
        today = datetime.now().strftime('%Y-%m-%d')
        
        # Get data from database
        postgres = PostgresHook(postgres_conn_id='postgres_default')
        query = """
        SELECT 
            name,
            price_usd,
            price_thb,
            market_cap,
            volume,
            created_at
        FROM cryptocurrency 
        WHERE DATE(created_at) = CURRENT_DATE
        ORDER BY market_cap DESC;
        """
        
        df = postgres.get_pandas_df(query)
        
        # 1. Export to CSV
        csv_path = export_dir / f'crypto_prices_{today}.csv'
        df.to_csv(csv_path, index=False)
        
        # 2. Export to Excel
        excel_path = export_dir / f'crypto_prices_{today}.xlsx'
        with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
            # Main data sheet
            df.to_excel(writer, sheet_name='Prices', index=False)
            
            # Summary sheet
            summary = pd.DataFrame({
                'Metric': [
                    'Total Cryptocurrencies',
                    'Average Price (USD)',
                    'Total Market Cap (USD)',
                    'Date Generated'
                ],
                'Value': [
                    len(df),
                    f"${df['price_usd'].mean():,.2f}",
                    f"${df['market_cap'].sum():,.2f}",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ]
            })
            summary.to_excel(writer, sheet_name='Summary', index=False)
            
            # Format Excel
            workbook = writer.book
            money_fmt = workbook.add_format({'num_format': '$#,##0.00'})
            worksheet = writer.sheets['Prices']
            worksheet.set_column('B:C', 15, money_fmt)
            worksheet.set_column('D:E', 20, money_fmt)
        
        # 3. Generate HTML report
        html_path = export_dir / f'crypto_report_{today}.html'
        
        # Create visualizations
        fig1 = px.bar(
            df,
            x='name',
            y=['price_usd', 'price_thb'],
            title='Cryptocurrency Prices (USD vs THB)',
            barmode='group'
        )
        
        fig2 = px.pie(
            df,
            values='market_cap',
            names='name',
            title='Market Cap Distribution'
        )
        
        html_content = f"""
        <html>
            <head>
                <title>Crypto Market Report - {today}</title>
                <style>
                    body {{ font-family: Arial, sans-serif; padding: 20px; }}
                    table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
                    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                    th {{ background-color: #f2f2f2; }}
                </style>
            </head>
            <body>
                <h1>Cryptocurrency Market Report - {today}</h1>
                
                <h2>Market Overview</h2>
                {summary.to_html(index=False)}
                
                <h2>Price Comparison</h2>
                {fig1.to_html()}
                
                <h2>Market Cap Distribution</h2>
                {fig2.to_html()}
                
                <h2>Detailed Price Data</h2>
                {df.to_html(index=False)}
            </body>
        </html>
        """
        
        with open(html_path, 'w') as f:
            f.write(html_content)
        
        logger.info(f"""
        Export completed successfully:
        - CSV: {csv_path}
        - Excel: {excel_path}
        - HTML Report: {html_path}
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
    'crypto_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for cryptocurrency data with exports',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['crypto', 'etl']
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

export_reports = PythonOperator(
    task_id='export_reports',
    python_callable=export_data,
    dag=dag
)

create_table >> [scrape_crypto, fetch_exchange] >> transform_insert >> export_reports