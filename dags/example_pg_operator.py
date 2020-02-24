from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import DAG
from datetime import datetime, timedelta

two_days_ago = datetime.combine(datetime.today() - timedelta(2),
                                  datetime.min.time())
args = {
    'owner': 'tung',
    'start_date': two_days_ago,
    'email': 'tung@flownote.ai'
}

dag = DAG(
    dag_id='example_pg_operator',
    default_args=args,
    schedule_interval='0 0 * * *',
    catchup=False
)

sql = \
"""
DROP TABLE IF EXISTS {table_name};
CREATE TABLE {table_name} (
   {table_name}_id INT NOT NULL PRIMARY KEY,
   firstname VARCHAR (50),
   lastname VARCHAR (50)
);
"""
tables = ['one', 'two', 'three', 'four']

for table in tables:
  operator = PostgresOperator(
      task_id='create_{}'.format(table),
      dag=dag,
      postgres_conn_id='airflow_pg',
      sql=sql.format(table_name=table),
      database='airflow',
  )
