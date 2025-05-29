from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Import your custom API class
from api.api import API

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

def daily_refresh():
    api = API()
    results = api.invoke_daily_update()
    logger.info("Triggered daily update")

with DAG(
    dag_id='trigger_custom_api_workflow',
    default_args=default_args,
    description='Trigger custom API standard workflow',
    start_date=datetime(2025, 5, 29),
    schedule_interval='0 3 * * *', #Every day at 3 am
    catchup=False,
) as dag:

    run_daily_refresh = PythonOperator(
        task_id='daily-refresh',
        python_callable=daily_refresh
    )

    run_daily_refresh
