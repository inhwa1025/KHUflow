from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

import pathlib
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta


default_args = {
    'description': 'Sample Spark Dag',
    'depend_on_past': False,
    'start_date': days_ago(1),
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': 'inhwa.jo@nhn.com',
}

dag = DAG(
    # DAG의 이름
    'sample-spark-dag',
    # DAG에서 사용할 기본적인 파라미터 값
    default_args=default_args,
    # DAG가 언제 실행 될 지 설정. "@once" : 한 번만 실행
    schedule_interval="@once",
    tags=["sample"],
)

start_dag = DummyOperator(
    task_id='start_dag',
    dag=dag
)

end_dag = DummyOperator(
    task_id='end_dag',
    dag=dag
)

t1 = SparkKubernetesOperator(
    task_id='sample-spark-task',
    namespace="airflow",
    application_file=pathlib.Path("/opt/airflow/dags/repo/dags/sample-sko-spark.yaml").read_text(),
    kubernetes_conn_id="kubernetes_default", #ns default in airflow connection UI
    do_xcom_push=True,
    dag=dag
)

t2 = SparkKubernetesSensor(
    task_id='sample-spark-log',
    namespace="airflow",
    application_name="{{ task_instance.xcom_pull(task_ids='task1-spark')['metadata']['name'] }}",
    kubernetes_conn_id="kubernetes_default", #ns default in airflow connection UI
    attach_log=True,
    dag=dag,
)

start_dag >> t1 >> t2 >> end_dag
