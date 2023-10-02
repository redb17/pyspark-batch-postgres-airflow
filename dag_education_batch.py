from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import subprocess
from airflow.operators.email import EmailOperator


params = {
    'spark_conf': '/path/to/spark.conf',
    'pyspark_main': '/path/to/education_batch.py'
}


def run_pyspark_job(ti):
    today_date = datetime.now().strftime("%Y-%m-%d")

    result = subprocess.run(['spark-submit', '--properties-file', params['spark_conf'], 
        params['pyspark_main'], '--date', today_date, '--conf_file', params['spark_conf']],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    email_msg = f"""
        <p>Airflow Job! Here's the spark_stdout and spark_stderr message.</p>
        <p>{result.stdout}</p>
        <p>{result.stderr}</p>
    """

    ti.xcom_push(key='email_msg', value=email_msg)
    

default_args = {
    'owner': 'redb17',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1)
}

dag = DAG(
    dag_id='my_pyspark_dag',
    default_args=default_args,
    schedule='0 23 * * *', # daily at 23:00
    catchup=False
)

pyspark_task = PythonOperator(
    task_id='pyspark_task',
    python_callable=run_pyspark_job,
    dag=dag,
)

email_task = EmailOperator(
    task_id='email_task',
    to='email@example.com',
    subject='FROM AIRFLOW: PySpark Job',
    html_content="{{ti.xcom_pull(key='email_msg', task_ids='pyspark_task')}}",
    trigger_rule='all_done',
    dag=dag
)

pyspark_task >> email_task
