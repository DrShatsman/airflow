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

dag = DAG('test1', default_args=default_args, schedule_interval= '0 * * * *', catchup=True,
          max_active_tasks=1, max_active_runs=1)



task1 = BashOperator(
    task_id='extract_data_and_load_into_temp_table',
    bash_command='python3 /airflow/scripts/test1/task1.py',
    dag=dag)


task1