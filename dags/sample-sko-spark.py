"""
This is an example DAG which uses SparkKubernetesOperator and SparkKubernetesSensor.
In this example, we create two tasks which execute sequentially.
The first task is to submit sparkApplication on Kubernetes cluster(the example uses spark-pi application).
and the second task is to check the final state of the sparkApplication that submitted in the first state.
Spark-on-k8s operator is required to be already installed on Kubernetes
https://github.com/GoogleCloudPlatform/spark-on-k8s-operator
"""

from datetime import datetime, timedelta

# [START import_module]
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

# etc
import pathlib
# [END import_module]


# [START instantiate_dag]

dag = DAG(
    'sample-sko-spark',
    default_args={'max_active_runs': 1},
    description='submit spark-pi as sparkApplication on kubernetes',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['sample']
)

############################
# Pod
############################
# $ kubectl get pod -n airflow | grep sko
# sample-sko-spark-20211125t012827-1-5bd3597d54b5a9d4-exec-1   1/1     Running     0          14s
# sample-sko-spark-20211125t012827-1-5bd3597d54b5a9d4-exec-2   1/1     Running     0          14s
# sample-sko-spark-20211125t012827-1-5bd3597d54b5a9d4-exec-3   1/1     Running     0          14s
# sample-sko-spark-20211125t012827-1-driver                    1/1     Running     0          26s

t1 = SparkKubernetesOperator(
    task_id='task1-spark',
    namespace="airflow",
    application_file=pathlib.Path("/opt/airflow/dags/repo/dags/sample-sko-spark.yaml").read_text(), #officially know bug
    kubernetes_conn_id="kubernetes_default", #ns default in airflow connection UI
    do_xcom_push=True,
    dag=dag
)

############################
# logs
############################
# saving log from executors 
# $ k get pod -n airflow | grep log
# sampleskosparksampleskosparklog.7d21881b74bd4c3180d8a470ff4257a5   1/1     Running   0          42s

t2 = SparkKubernetesSensor(
    task_id='sample-sko-spark-log',
    namespace="airflow",
    application_name="{{ task_instance.xcom_pull(task_ids='task1-spark')['metadata']['name'] }}",
    kubernetes_conn_id="kubernetes_default", #ns default in airflow connection UI
    attach_log=True, #
    dag=dag,
)

t1 >> t2
