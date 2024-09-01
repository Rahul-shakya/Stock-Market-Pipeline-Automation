from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.decorators import dag, task
from datetime import datetime
from airflow.operators.python_operator import PythonOperator

def my_callable(*args, **kwargs):
    print("Hello from PythonOperator")


def factory_subdag(parent_dag_name, child_dag_name, default_args):

    dag = DAG(
        dag_id=f'{parent_dag_name}.{child_dag_name}',  
        default_args = default_args)
    
    # preprocess_files = BashOperator(
    #     task_id='preprocess_files',
    #     bash_command='echo hello',
    #     dag=dag)
    
    # preprocess_db_data = BashOperator(
    #     task_id='preprocess_db',
    #     bash_command='echo yo',
    #     dag=dag)

    python_task1 = PythonOperator(
        task_id='my_python_task1',
        python_callable=my_callable,
        dag = dag
    )

    python_task2 = PythonOperator(
        task_id='my_python_task2',
        python_callable=my_callable,
        dag = dag
    )

    
    return dag
