import airflow
from subdags.subdag import factory_subdag
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.executors.sequential_executor import SequentialExecutor
from airflow.executors.celery_executor import CeleryExecutor
from datetime import datetime

DAG_NAME="test_subdag"

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2024, 9, 1),
}

with DAG(dag_id=DAG_NAME, default_args=default_args, schedule_interval="@once") as dag:
    start = DummyOperator(
        task_id='startnow'
    )

    subdag_1 = SubDagOperator(
        task_id='subdag-1',
        subdag=factory_subdag(DAG_NAME, 'subdag-1', default_args)
    )

    some_other_task = DummyOperator(
        task_id='check'
        )

    subdag_2 = SubDagOperator(
        task_id='subdag-2',
        subdag=factory_subdag(DAG_NAME, 'subdag-2', default_args)
    )

    end = DummyOperator(
        task_id='final'
    )

    start >> subdag_1 >> some_other_task >> subdag_2 >> end