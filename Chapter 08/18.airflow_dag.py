from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.dates import days_ago

dag = DAG(
  'spark_job_example',
  description='Another Spark job example',
  schedule_interval='0 12 * * *',
  start_date=days_ago(1),
  catchup=False)

spark_job = SparkSubmitOperator(
  task_id='spark_job',
  application='/path/to/your_spark_job.py', # Path to your Spark job
  conn_id='spark_default',
  dag=dag
)
