import sys
import string
import random
import os

from airflow import DAG  # noqa
from airflow.operators.python_operator import PythonOperator  # noqa
from datetime import datetime, timedelta
from pyhocon import ConfigFactory

from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader

from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.airflow_task import AirflowTask
from databuilder.transformer.base_transformer import NoopTransformer
from databuilder.transformer.airflow_transformer import AirflowTransformer
from databuilder.extractor.kafka_source_extractor import KafkaSourceExtractor
from databuilder.models.multi_table_metadata import MultiTableMetadata

neo_host = os.getenv('NEO_HOST', 'neo4j.default.svc.cluster.local')
neo4j_user = os.getenv('NEO_USER', 'neo4j')
neo4j_password = os.getenv('NEO_PASS', 'test')

NEO4J_ENDPOINT = 'bolt://{}:7687'.format(neo_host)
neo4j_endpoint = NEO4J_ENDPOINT

kafka_group_id = os.getenv('KAFKA_GROUP_ID', 'amundsen')

def random_string(stringLength=8):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))

def create_job(transformer=AirflowTransformer()):
    tmp_folder = '/var/tmp/amundsen/{}'.format(random_string())
    node_files_folder = '{tmp_folder}/nodes'.format(tmp_folder=tmp_folder)
    relationship_files_folder = '{tmp_folder}/relationships'.format(tmp_folder=tmp_folder)

    kafka_extractor = KafkaSourceExtractor()
    csv_loader = FsNeo4jCSVLoader()
    task = AirflowTask(
        extractor=kafka_extractor,
        loader=csv_loader,
        transformer=transformer,
    )

    consumer_config = {
        '"group.id"': kafka_group_id,
        '"enable.auto.commit"': False,
        '"bootstrap.servers"': 'bootstrap.kafka.svc.cluster.local:9092',
        '"auto.offset.reset"': 'earliest',
    }
    job_config = ConfigFactory.from_dict({
        'extractor.kafka_source.consumer_config': consumer_config,
        'extractor.kafka_source.{}'.format(KafkaSourceExtractor.RAW_VALUE_TRANSFORMER):
            'databuilder.transformer.base_transformer.NoopTransformer',
        'extractor.kafka_source.{}'.format(KafkaSourceExtractor.TOPIC_NAME_LIST): ['airflow-sql'],
        'extractor.kafka_source.{}'.format(KafkaSourceExtractor.TRANSFORMER_THROWN_EXCEPTION): True,
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.NODE_DIR_PATH):
            node_files_folder,
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.RELATION_DIR_PATH):
            relationship_files_folder,
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.SHOULD_DELETE_CREATED_DIR):
            True,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NODE_FILES_DIR):
            node_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.RELATION_FILES_DIR):
            relationship_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_END_POINT_KEY):
            neo4j_endpoint,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_USER):
            neo4j_user,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_PASSWORD):
            neo4j_password,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.JOB_PUBLISH_TAG):
            'airflow-sql',  # should use unique tag here like {ds}
    })

    publisher = Neo4jCsvPublisher()
    publisher.register_call_back(kafka_extractor)

    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=publisher)
    job.launch()

dag_args = {
    # 4AM, 4PM PST
    'schedule_interval': '@once'
}

two_days_ago = datetime.combine(datetime.today() - timedelta(2), datetime.min.time())

default_args = {
    'owner': 'amundsen',
    'start_date': two_days_ago,
    'depends_on_past': False,
    'email': [''],
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='amundsen_neo4j',
    default_args=default_args,
    **dag_args,
)

amundsen_databuilder_neo4j_job = PythonOperator(
    dag=dag,
    task_id='amundsen_databuilder_neo4j_job',
    python_callable=create_job
)
