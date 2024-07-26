import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator

import extract
import load
import transform

args = {
    'owner': 'Archies Desai',
    'email': ['email@mail.com'],
    'email_on_failure': True,
    'start_date': datetime.datetime(2024, 1, 1, tz="UTC") # make start date in the past
}

#defining the dag object
dag = DAG(
    dag_id='etl-pipeline',
    default_args=args,
    schedule_interval='@daily' # to run our pipeline daily
)

#create a DAG with a simple three step process that form our pipeline: extract, transform and load
with dag:
    # define a task to run the extract function
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract.extract
    )

    # define a task to run the transform function
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform.transform
    )

    # define a task to run the load function
    load_task = PythonOperator(
        task_id='load',
        python_callable=load.load
    )

# define the run time dependency: run the extract_task first, then the transform_task and finally the load_task
    extract_task >> transform_task >> load_task
