from airflow.operators.generic_transfer import GenericTransfer
from airflow.models import DAG
from datetime import datetime, timedelta

two_days_ago = datetime.combine(datetime.today() - timedelta(2),
                                  datetime.min.time())
args = {
    'owner': 'viet',
    'start_date': two_days_ago,
    'email': 'viet@flownote.ai'
}

dag = DAG(
    dag_id='example_generic_transfer',
    default_args=args,
    schedule_interval='0 0 * * *',
    catchup=False
)

sql = "SELECT * FROM holy;"

generic_transfer = GenericTransfer(
    task_id='generic_transfer',
    dag=dag,
    source_conn_id='airflow_pg',
    destination_conn_id='airflow_pg',
    destination_table='sensei',
    sql=sql,
)
