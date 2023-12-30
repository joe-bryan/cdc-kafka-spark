import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


dag = DAG(
    dag_id="spark_flow",
    default_args={"owner": "your_name", "start_date": airflow.utils.dates.days_ago(1)},
    schedule_interval="@daily",
)

start = PythonOperator(
    task_id="start", python_callable=lambda: print("Jobs started"), dag=dag
)

spark_job = SparkSubmitOperator(
    task_id="spark_job",
    conn_id="spark_connection",
    application="jobs/spark_s3.py",
    packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
    "org.apache.kafka:kafka-clients:3.5.0,"
    "org.apache.hadoop:hadoop-aws:3.2.0,"
    "com.amazonaws:aws-java-sdk-s3:1.12.600",
    dag=dag,
)

end = PythonOperator(
    task_id="end", python_callable=lambda: print("Job completed successfully"), dag=dag
)

start >> spark_job >> end
