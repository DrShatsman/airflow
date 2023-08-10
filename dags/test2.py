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
    "start_date": datetime(2023, 7, 31),
    "retries": 1,
    "max_active_runs": 1
    #"retry_delay": timedelta(minutes=0.1)
}

dag = DAG('test2', default_args=default_args, schedule= '0 7 * * *', catchup=True,
          max_active_tasks=5, max_active_runs=1)



task1 = BashOperator(
    task_id='clean_temp_table',
    bash_command='python3 /airflow/scripts/test2/task1.py',
    dag=dag)

task2 = BashOperator(
    task_id='extract_data_and_load_into_temp_table',
    bash_command='python3 /airflow/scripts/test2/task2.py',
    dag=dag)

task3 = BashOperator(
    task_id='check_data_in_the_temp_table',
    bash_command='python3 /airflow/scripts/test2/task3.py',
    dag=dag)

task4 = BashOperator(
    task_id='load_data_into_table',
    bash_command='python3 /airflow/scripts/test2/task4.py',
    dag=dag)

task5 = BashOperator(
    task_id='again_clean_temp_table',
    bash_command='python3 /airflow/scripts/test2/task5.py',
    dag=dag)


task1>>task2>>task3>>task4>>task5