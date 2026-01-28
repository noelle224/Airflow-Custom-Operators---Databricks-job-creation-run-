from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
import os
import boto3
import typing


class PostgresToS3Operator(BaseOperator):
    def __init__(
            self,
            sql:str,
            bucket:str,
            key:str,
            postgres_conn_id:str='postgres',
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.bucket = bucket
        self.key = key
        self.postgres_conn_id = postgres_conn_id

        
    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        s3_hook = boto3.client('s3')

        results = postgres_hook.get_records(sql=self.sql)
        csv_data = "\n".join([",".join(map(str, row)) for row in results])
        bucket_name = self.bucket
        object_key = self.key
        s3_hook.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=csv_data,
            ContentType="text/csv"
        )
        print(f"Data uploaded to s3://{bucket_name}/{object_key}")


   









