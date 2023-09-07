from datetime import datetime
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime as dt



#connection = BaseHook.get_connection("miner_trg1")

default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": datetime(2023, 7, 17),
    "retries": 1,
    "max_active_runs": 1
    #"retry_delay": timedelta(minutes=0.1)
}

dag = DAG('load_weather_daily', default_args=default_args, schedule_interval= '0 12 * * *', catchup=True,
          max_active_tasks=5, max_active_runs=1)



clean_temp_table = BashOperator(
    task_id='clean_temp_table',
    bash_command='python3 /airflow/scripts/load_weather_daily/clean_temp_table.py',
    dag=dag)

extract_data_and_load_into_temp_table = BashOperator(
    task_id='extract_data_and_load_into_temp_table',
    bash_command='python3 /airflow/scripts/load_weather_daily/extract_data_and_load_into_temp_table.py',
    dag=dag)

check_data_in_the_temp_table = BashOperator(
    task_id='check_data_in_the_temp_table',
    bash_command='python3 /airflow/scripts/load_weather_daily/check_data_in_the_temp_table.py',
    dag=dag)

load_data_into_table = BashOperator(
    task_id='load_data_into_table',
    bash_command='python3 /airflow/scripts/load_weather_daily/load_data_into_table.py',
    dag=dag)

again_clean_temp_table = BashOperator(
    task_id='again_clean_temp_table',
    bash_command='python3 /airflow/scripts/load_weather_daily/again_clean_temp_table.py',
    dag=dag)


clean_temp_table>>extract_data_and_load_into_temp_table>>check_data_in_the_temp_table>>load_data_into_table>>again_clean_temp_table