import os
#os.environ['PYSPARK_PYTHON'] = 'python3'
#os.environ['PYSPARK_DRIVER_PYTHON'] = 'python3'
from datetime import timedelta
import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(0),
}

dag = DAG(
    dag_id='example_bash_operator',
    default_args=args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60),
)
run_extract_data =  BashOperator(
    task_id='run_extract_data',
    bash_command='/usr/bin/python3 /opt/airflow/dags/crawl.py',
    dag=dag)

run_spark_job = SparkSubmitOperator(
    task_id='spark_submit_task',
    conn_id='spark_default', 
    application='dags/spark.py',  
    verbose=1, 
    dag=dag,    
)
# load_raw_data = BashOperator(
#     task_id='load_raw_data',
#     bash_command='sudo docker exec setupenvironment_jupyterlab_1 /bin/sh -c "python3 load_raw_data.py"',
#     dag=dag,
# )
# run_connect_spark_job >> load_raw_data

# handle_missing = BashOperator(
#     task_id='handle_missing',
#     bash_command='sudo docker exec setupenvironment_jupyterlab_1 /bin/sh -c "python3 handle_missing.py"',
#     dag=dag,
# )
# load_raw_data >> handle_missing
# nomalize_datatype = BashOperator(
#     task_id='nomalize_datatype',
#     bash_command='sudo docker exec setupenvironment_jupyterlab_1 /bin/sh -c "python3 nomalize_data.py"',
#     dag=dag,
# )
# handle_missing >> nomalize_datatype
# remove_duplicate = BashOperator(
#     task_id='remove_duplicate',
#     bash_command='sudo docker exec setupenvironment_jupyterlab_1 /bin/sh -c "python3 remove_duplicate.py"',
#     dag=dag,
# )
# nomalize_datatype >> remove_duplicate


# load_data = BashOperator(
#     task_id='load_data',
#     bash_command='sudo docker exec setupenvironment_jupyterlab_1 /bin/sh -c "python3 load_data.py"',
#     dag=dag,
# )

# remove_duplicate >> load_data
run_extract_data >> run_spark_job

if __name__ == "__main__":
    dag.cli()