from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import pandas as pd

import smtplib

# SQL to create the attendance table
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS intern_attendance (
    id SERIAL PRIMARY KEY,
    date DATE,
    arrival_time TIMESTAMP,
    departure_time TIMESTAMP
);
"""

# Record the arrival time
def record_arrival(**context):
    execution_date = context['execution_date']
    current_date = execution_date.strftime('%Y-%m-%d')
    arrival_time = f"{current_date} 09:00:00"

    pg_hook = PostgresHook(postgres_conn_id='Noey_Interns')
    insert_sql = """
    INSERT INTO intern_attendance (date, arrival_time)
    VALUES (%s, %s)
    RETURNING id;
    """

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(insert_sql, (current_date, arrival_time))
            record_id = cur.fetchone()[0]
            conn.commit()
            context['task_instance'].xcom_push(key='record_id', value=record_id)

# Record the departure time
def record_departure(**context):
    execution_date = context['execution_date']
    current_date = execution_date.strftime('%Y-%m-%d')
    departure_time = f"{current_date} 18:00:00"
    record_id = context['task_instance'].xcom_pull(key='record_id')

    pg_hook = PostgresHook(postgres_conn_id='Noey_Interns')
    update_sql = """
    UPDATE intern_attendance
    SET departure_time = %s
    WHERE id = %s;
    """

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(update_sql, (departure_time, record_id))
            conn.commit()

# Generate the report
def generate_report(**context):
    current_month = datetime.now().strftime('%Y-%m')
    execution_date = context['execution_date']
    current_date = execution_date.strftime('%Y-%m-%d')

    pg_hook = PostgresHook(postgres_conn_id='Noey_Interns')
    query = f"""
    SELECT 
        date,
        arrival_time::TIME as arrival,
        departure_time::TIME as departure,
        EXTRACT(EPOCH FROM (departure_time - arrival_time))/3600 as hours_worked
    FROM intern_attendance 
    WHERE TO_CHAR(date, 'YYYY-MM') = '{current_month}'
    ORDER BY date;
    """

    df = pg_hook.get_pandas_df(query)

    summary = {
        'date': current_date,
        'total_days': len(df),
        'avg_hours': round(df['hours_worked'].mean(), 2) if not df.empty else 0,
        'on_time_arrivals': len(df[df['arrival'] <= pd.Timestamp('09:00:00').time()]) if not df.empty else 0
    }

    email_content = f"""
    <h2>Daily Attendance Report - {summary['date']}</h2>
    <h3>Summary for Current Month:</h3>
    <ul>
        <li>Total Working Days: {summary['total_days']}</li>
        <li>Average Hours Worked: {summary['avg_hours']} hours</li>
        <li>On-time Arrivals: {summary['on_time_arrivals']} days</li>
    </ul>
    <p>All attendance records have been successfully processed and saved.</p>
    """

    context['task_instance'].xcom_push(key='email_content', value=email_content)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['noansrnn@gmail.com'],  # Replace with your recipient email
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 1, 6)
}

# Define the DAG
with DAG(
    'noey_interns',
    default_args=default_args,
    schedule_interval='0 9 * * 1-5',
    catchup=False
) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='Noey_Interns',
        sql=CREATE_TABLE_SQL
    )

    arrival = PythonOperator(
        task_id='record_arrival',
        python_callable=record_arrival,
        provide_context=True
    )

    departure = PythonOperator(
        task_id='record_departure',
        python_callable=record_departure,
        provide_context=True
    )

    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
        provide_context=True
    )

  
    # send_email = EmailOperator(
    # task_id='send_email',
    # to=['noansrnn@gmail.com'],  # Replace with your recipient email
    # subject='Attendance Report - {{ ds }}',
    # html_content="{{ task_instance.xcom_pull(key='email_content') }}",
    # conn_id='nnemail'  # Use the connection ID configured in Airflow
    # )


    # Define task dependencies
    create_table >> arrival >> departure >> generate_report  # >> send_email
