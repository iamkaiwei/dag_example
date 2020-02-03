from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import DAG
from datetime import datetime, timedelta

two_days_ago = datetime.combine(datetime.today() - timedelta(2),
                                  datetime.min.time())
args = {
    'owner': 'airflow',
    'start_date': two_days_ago,
}

dag = DAG(
    dag_id='example_pg_operator',
    default_args=args,
    schedule_interval='0 0 * * *'
)

sql = \
"""
DROP TABLE IF EXISTS holy;
CREATE TABLE holy (
   holy_id INT NOT NULL PRIMARY KEY,
   firstname VARCHAR (50),
   lastname VARCHAR (50)
);
"""

run_this_alter = PostgresOperator(
    task_id='run_this_alter',
    dag=dag,
    postgres_conn_id='airflow_pg',
    sql=sql,
    database='airflow',
)
