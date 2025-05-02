import logging
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import pytz

def get_date_range():
    bangkok_tz = pytz.timezone("Asia/Bangkok")

    today = datetime.now().strftime("%Y%m%d")  
    from_date = (datetime.now() - timedelta(days=6)).strftime("%d/%m/%Y")  
    end_date = datetime.now().strftime("%d/%m/%Y")  
    return today, from_date, end_date

def is_api_available(**kwargs):
    today, from_date, end_date = get_date_range()
    url = f"https://api.settrade.com/api/fund-nav/all?fromDate={from_date}&toDate={end_date}"
    response = requests.get(url)
    logging.info(f"API response status code: {response.status_code}")
    return response.status_code == 200

def ODS_fetch_and_insert_nav(**kwargs):
    
    today, from_date, end_date = get_date_range()
    url = f"https://api.settrade.com/api/fund-nav/all?fromDate={from_date}&toDate={end_date}"
    
    try:
        fund_navs = requests.get(url).json().get("fundNavs", [])
        logging.info(f"Processing {len(fund_navs)} NAV records")
        
        pg_hook = PostgresHook(postgres_conn_id="SESAME-DB")
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM ods_thaimutualfundnav_dtl_di WHERE inc_day = %s", (today,))
                insert_data = [
                    (
                        fund["fundConnextId"], fund["symbol"], fund["nameTh"], fund["nameEn"],
                        fund["amcId"], fund["nav"], fund["navPerUnit"], fund["priorNavPerUnit"],
                        fund["change"], fund["navDate"], fund["buySwapPrice"], fund["sellSwapPrice"],
                        fund["buyPrice"], fund["sellPrice"], fund.get("dividendValue", None),
                        fund.get("dividendDate", None), fund.get("bookCloseDate", None), 
                        fund["projectType"], today 
                    ) for fund in fund_navs
                ]
                cursor.executemany("""
                    INSERT INTO ods_thaimutualfundnav_dtl_di (
                        fundconnextid, symbol, nameth, nameen, amcid, nav, navperunit,
                        priornavperunit, change, navdate, buyswapprice, sellswapprice,
                        buyprice, sellprice, dividendvalue, dividenddate, bookclosedate, 
                        projecttype, inc_day
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, insert_data)
                conn.commit()
                logging.info(f"First 5 records to be inserted: {insert_data[:5]}")
                logging.info(f"Successfully processed {len(fund_navs)} records")
    except Exception as e:
        logging.error(f"Error processing NAV data: {e}")
        raise
    
def DWD_fetch_ODS_and_insert():
    logging.info("Fetching ODS data and inserting/updating DWD.")

    pg_hook = PostgresHook(postgres_conn_id="SESAME-DB")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    try:
        _, from_date, end_date = get_date_range()

        cursor.execute("SELECT 1 FROM dwd_thaimutualfundnav_dtl_di LIMIT 1")
        dwd_has_data = cursor.fetchone()

        if not dwd_has_data:
            logging.info("DWD is empty. Copying all records from ODS.")
            cursor.execute("""
                INSERT INTO dwd_thaimutualfundnav_dtl_di (
                    fundconnextid, symbol, nameth, nameen, amcid, nav, navperunit,
                    priornavperunit, change, navdate, buyswapprice,
                    sellswapprice, buyprice, sellprice, dividendvalue,
                    dividenddate, bookclosedate, projecttype, inc_day
                )
                SELECT 
                    fundconnextid, symbol, nameth, nameen, amcid, nav, navperunit,
                    priornavperunit, change, navdate, buyswapprice,
                    sellswapprice, buyprice, sellprice, dividendvalue,
                    dividenddate, bookclosedate, projecttype, 
                    TO_CHAR(navdate, 'YYYYMMDD') as inc_day
                FROM ods_thaimutualfundnav_dtl_di
                """)
            logging.info(f"Inserted {cursor.rowcount} initial records into DWD.")

        else:
            logging.info("Processing new records and updates from ODS.")
            cursor.execute("""
                UPDATE dwd_thaimutualfundnav_dtl_di d
                SET 
                    symbol = o.symbol,
                    nameth = o.nameth,
                    nameen = o.nameen,
                    amcid = o.amcid,
                    nav = o.nav,
                    navperunit = o.navperunit,
                    priornavperunit = o.priornavperunit,
                    change = o.change,
                    buyswapprice = o.buyswapprice,
                    sellswapprice = o.sellswapprice,
                    buyprice = o.buyprice,
                    sellprice = o.sellprice,
                    dividendvalue = o.dividendvalue,
                    dividenddate = o.dividenddate,
                    bookclosedate = o.bookclosedate,
                    projecttype = o.projecttype,
                    inc_day = TO_CHAR(o.navdate, 'YYYYMMDD')
                FROM ods_thaimutualfundnav_dtl_di o
                WHERE d.fundconnextid = o.fundconnextid
                AND d.navdate = o.navdate
                AND o.navdate BETWEEN TO_DATE(%s, 'DD/MM/YYYY') 
                AND TO_DATE(%s, 'DD/MM/YYYY')
            """, (from_date, end_date))
            logging.info(f"Updated {cursor.rowcount} existing records in DWD.")

            cursor.execute("""
                INSERT INTO dwd_thaimutualfundnav_dtl_di (
                    fundconnextid, symbol, nameth, nameen, amcid, nav, navperunit,
                    priornavperunit, change, navdate, buyswapprice,
                    sellswapprice, buyprice, sellprice, dividendvalue,
                    dividenddate, bookclosedate, projecttype, inc_day
                )
                SELECT 
                    o.fundconnextid, o.symbol, o.nameth, o.nameen, o.amcid, o.nav, o.navperunit,
                    o.priornavperunit, o.change, o.navdate, o.buyswapprice,
                    o.sellswapprice, o.buyprice, o.sellprice, o.dividendvalue,
                    o.dividenddate, o.bookclosedate, o.projecttype, 
                    TO_CHAR(o.navdate, 'YYYYMMDD')
                FROM ods_thaimutualfundnav_dtl_di o
                WHERE NOT EXISTS (
                    SELECT 1 FROM dwd_thaimutualfundnav_dtl_di d
                    WHERE d.fundconnextid = o.fundconnextid
                    AND d.navdate = o.navdate
                )
                AND o.navdate BETWEEN TO_DATE(%s, 'DD/MM/YYYY') 
                AND TO_DATE(%s, 'DD/MM/YYYY')
            """, (from_date, end_date))
            logging.info(f"Inserted {cursor.rowcount} new records into DWD.")

        conn.commit()
        logging.info("DWD update completed successfully.")

    except Exception as e:
        conn.rollback()
        logging.error(f"Error during DWD update: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()
        
default_args = {
    "owner": "noey",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "etl_thaimutualfundnav_dtl_di",
    default_args=default_args,
    description="Thai mutual fund NAV ODS, DWD",
    schedule_interval="0 18 * * *",
    start_date=datetime(2025, 1, 28),
    catchup=False,
    tags=["etl", "nav", "api", "thai_mutual_fund", "thai", "noey"]
) as dag:

    check_api = ShortCircuitOperator(
        task_id="check_api_available",
        python_callable=is_api_available,
    )

    ods_fetch_and_insert = PythonOperator(
        task_id="ods_fetch_and_insert_nav",
        python_callable=ODS_fetch_and_insert_nav,
    )
    
    dwd_fetch_and_insert = PythonOperator(
        task_id="dwd_fetch_and_insert",
        python_callable=DWD_fetch_ODS_and_insert,
    )

    check_api >> ods_fetch_and_insert >> dwd_fetch_and_insert
    