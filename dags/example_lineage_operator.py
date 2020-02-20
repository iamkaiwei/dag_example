from airflow.operators.bash_operator import BashOperator
from airflow.models import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.lineage.datasets import DataSet

class Table(DataSet):
    type_name = "sql_table"
    attributes = ["database", "schema", "cluster", "table"]

args = {
    'owner': 'Tung',
    'start_date': days_ago(2),
    'email': 'tung@flownote.ai'
}

dag = DAG(
    dag_id='example_lineage', default_args=args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60),
    catchup=False
)

datasets = {}
tables = ['one', 'two', 'three']
for table in tables:
    data = { 'database': 'airflow', 'schema': 'airflow', 'cluster': 'gold', 'table': table }
    datasets[table] = Table(table, data)

run_this = BashOperator(
    task_id='run_me_first', bash_command='echo 1', dag=dag,
    inlets={"datasets": [datasets['one']]},
    outlets={"datasets": [datasets['two']]}
)

run_this_last = BashOperator(
    task_id='run_this_last', bash_command='echo 1', dag=dag,
    inlets={"auto": True},
    outlets={"datasets": [datasets['three']]}
)

run_this.set_downstream(run_this_last)
