FROM apache/airflow:2.9.3
RUN pip install "apache-airflow-providers-databricks==5.0.0"
