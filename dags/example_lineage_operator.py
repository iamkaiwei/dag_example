from airflow.operators.bash_operator import BashOperator
from airflow.models import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta

args = {
    'owner': 'Airflow',
    'start_date': days_ago(2)
}

dag = DAG(
    dag_id='example_lineage', default_args=args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60),
    catchup=False
)

holy = { 'database': 'airflow', 'schema': 'airflow', 'cluster': 'gold', 'table': 'holy' }
sensei = { 'database': 'airflow', 'schema': 'airflow', 'cluster': 'gold', 'table': 'sensei' }

run_this = BashOperator(
    task_id='run_me_first', bash_command='echo 1', dag=dag,
    inlets={"datasets": [holy]},
    outlets={"datasets": [sensei]}
)