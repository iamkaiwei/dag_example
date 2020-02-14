# import kubernetes.client.models as k8s
# from airflow.contrib.kubernetes.secret import Secret
# from airflow.contrib.kubernetes.volume import Volume
# from airflow.contrib.kubernetes.volume_mount import VolumeMount
# from airflow.contrib.kubernetes.pod import Port

from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

two_days_ago = datetime.combine(datetime.today() - timedelta(2),
                                  datetime.min.time())
default_args = {
    'owner': 'airflow',
    'start_date': two_days_ago,
}

dag = DAG(
    dag_id='kubernetes_sample',
    default_args=default_args,
    schedule_interval=timedelta(minutes=30),
    catchup=False
)

k8s_operator = KubernetesPodOperator(
    namespace='default',
    task_id='gitgud',
    dag=dag,
    image='amundsenbuilder:0.0.1',
    labels={'foo': 'bar'},
    cmds=['python', 'amundsen_neo4j_operator.py'],
    name='test',
    in_cluster=True,
)