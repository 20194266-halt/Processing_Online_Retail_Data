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
run_extract_data >> run_spark_job
if __name__ == "__main__":
    dag.cli()