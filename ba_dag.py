
from datetime import datetime, timedelta
import pytz
import configparser

config = configparser.ConfigParser()
config.read("creds.ini")


# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from update_batting_avg import run_pull

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020,7,29,14,0),
    'email': config[info]['email'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=20),

}
## since cron is utc, this will be 10 AM EDT, 9 AM EST (which doesn't really matter since baseball season is all during DST)
dag = DAG(
    dag_id='batting_average',
    default_args=default_args,
    description='A DAG to pull statcast data and regenerate batting averages to feed the batting average tool daily',
    schedule_interval=schedule_interval="0 14 * * *",
)

## making the function to run
yesterday=(datetime.now(pytz.timezone('US/Eastern')) - timedelta(1)).strftime('%Y-%m-%d')

# def puller_function(start_date):
#     run_pull(start_date)

run_puller=PythonOperator(
                            task_id='ba_pull',
                            provide_context=True,
                            python_callable=run_pull,
                            op_args={'start_date':yesterday}
                            dag=dag,
                            )

run_puller
