from datetime import datetime
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime as dt


default_args = {
    "owner": "elt_user",
    "depends_on_past": False,
    "start_date": datetime(2023, 8, 18),
    "retries": 1,
    "max_active_runs": 1
    #"retry_delay": timedelta(minutes=0.1)
}

dag = DAG('weed', default_args=default_args, schedule= '0 0,12 * * *', catchup=True,
          max_active_tasks=1, max_active_runs=1)



weed_extract_and_load = BashOperator(
    task_id='extract_and_load_data',
    bash_command='python3 /airflow/scripts/weed/weed_extract_and_load.py',
    dag=dag)





weed_extract_and_load