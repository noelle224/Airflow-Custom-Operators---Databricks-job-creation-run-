from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from connectors.postgres_s3_connector import PostgresToS3Operator
from connectors.YoutubeAPI_Operator import YoutubeAPISearchOperator
from connectors.DatabricksJobCreateOperator import DatabricksJobCreateOperator
from connectors.DatabricksJobRunOperator import DatabricksJobRunOperator
from connectors.DatabricksGetJobids import DatabricksGetJobids
from airflow.operators.python import ShortCircuitOperator
from Job_dictionary import Job_dictionary
import json

JOB_FILE_PATH = "/opt/airflow/dags/Job_dictionary.py"


maindag = DAG(
    dag_id='kitedag',
    schedule='@daily',
    start_date=datetime(2025, 12, 5),
    catchup=False
)

def update_status(job_name: str):
    """
    Updates Job_dictionary.py so that job.status = 1 when job completes.
    """
    # Read the current file
    with open(JOB_FILE_PATH, "r") as f:
        content = f.read()

    # Convert dict-like structure to Python object
    exec_globals = {}
    exec(content, exec_globals)
    jobs = exec_globals["Job_dictionary"]

    # Update matching job
    for j in jobs:
        if j["name"] == job_name:
            j["status"] = 1

    # Write file back
    new_content = "Job_dictionary = " + json.dumps(jobs, indent=4)
    with open(JOB_FILE_PATH, "w") as f:
        f.write(new_content)

def print_hello():
    print("Hello from Kite DAG!")

start = EmptyOperator(
    task_id = 'start_task',
    dag = maindag
)

hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=maindag
)

youtube_search_task = YoutubeAPISearchOperator(
    task_id= 'youtube_search_task',
    query = 'Stocks predictions',
    max_results=5,
    bucket='bhavikabucket22',
    key='youtube_data/search_result.json',
    youtube_conn_id='youtube_connection',
    function_name='search_videos',
    dag=maindag
)

youtube_get_category_task = YoutubeAPISearchOperator(
    task_id= 'youtube_get_category_task',
    query = None,
    max_results=100,
    bucket='bhavikabucket22',
    key='youtube_data/categories.json',
    youtube_conn_id='youtube_connection',
    function_name='get_categories',
    dag=maindag
)

previous_task = youtube_get_category_task

for job in Job_dictionary:

    job_name = job["name"]
    job_path = job["path"]
    job_taskkey = job["task_key"]
    job_status = job["status"]

    if job_status == 0:
        Transformation_job_creation = DatabricksJobCreateOperator(
            task_id = f"create_job_{job['id']}",
            job_name = job_name,
            task_key = job_taskkey,
            notebook_path = job_path
        )

        update_status_task = PythonOperator(
            task_id=f"update_status_{job_taskkey}",
            python_callable=update_status,
            op_kwargs={"job_name": job_name},
            dag=maindag
        )

        previous_task >> Transformation_job_creation >> update_status_task
        previous_task = update_status_task

Get_job_ids = DatabricksGetJobids(
    task_id="get_job_ids",
    dag=maindag
)
    
Transformation_job_run = DatabricksJobRunOperator(
    task_id="run_job",
    job_ids="{{ ti.xcom_pull('get_job_ids') }}"
    )

end = EmptyOperator(
    task_id = 'end_task',
    dag = maindag
)

start >> hello_task >> youtube_search_task >> youtube_get_category_task
previous_task >> Get_job_ids >> Transformation_job_run  >> end



