from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta


default_args = {
    'description': 'Redis 새로운 버전 감지 및 Variable 크롤링',
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
    'dynamic_task_test_0322',
    # DAG에서 사용할 기본적인 파라미터 값
    default_args=default_args,
    # DAG가 언제 실행 될 지 설정. "@once" : 한 번만 실행
    schedule_interval="@once",
    tags=["test"],
)

start_dag = DummyOperator(
    task_id='start_dag',
    dag=dag
)

end_dag = DummyOperator(
    task_id='end_dag',
    dag=dag
)

print_pwd = BashOperator(
    task_id='print_pwd',
    bash_command='echo $pwd',
    do_xcom_push=True,
    dag=dag
)

start_dag >> print_pwd >> end_dag
