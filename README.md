✅ Metadata-Driven Job Orchestration

Transformation jobs are defined in a centralized Job Dictionary

Airflow dynamically creates Databricks jobs based on job metadata

Easily add/remove jobs without modifying DAG logic

✅ Custom Airflow Operators

YouTube API Operator – Extracts video search results & category metadata

Databricks Job Create Operator – Creates Databricks jobs dynamically

Databricks Job Run Operator – Executes jobs using retrieved job IDs

Databricks Get Job IDs Operator – Fetches job IDs via XComs

✅ Stateful & Idempotent Execution

Job status is updated after successful execution

Prevents duplicate processing across DAG runs

✅ Clean Task Dependency Management

Uses EmptyOperator for DAG boundaries

Controlled task chaining ensures reliable execution order

--------------------------------------------------------------
🔄 DAG Workflow

Start DAG

Execute a sample Python task

Extract YouTube video search data → store in S3

Fetch YouTube video categories → store in S3

Dynamically create Databricks transformation jobs

Update job status after successful creation

Retrieve Databricks job IDs

Execute Databricks jobs

End DAG

📈 Use Cases

Automated ingestion of external API data

Metadata-driven transformation pipelines

Orchestrating Databricks workloads using Airflow

Building scalable, production-ready ETL systems

🔐 Configuration Notes

YouTube API credentials configured via Airflow connections

AWS credentials handled through Airflow connections / IAM roles

Databricks authentication managed via Airflow connections
