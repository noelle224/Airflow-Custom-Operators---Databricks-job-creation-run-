# ...existing code...
from airflow.models import BaseOperator
from customHooks.youtubeApiHook import YoutubeApiHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json

class YoutubeAPISearchOperator(BaseOperator):
    def __init__(
          self, 
          query, 
          max_results, 
          youtube_conn_id='youtube_connection', 
          bucket:str = None, 
          key:str = None, 
          function_name:str = None,
          *args, 
          **kwargs
          ):
        super().__init__(*args, **kwargs)
        self.query = query
        self.max_results = max_results
        self.youtube_conn_id = youtube_conn_id
        self.function_name = function_name
        self.bucket = bucket
        self.key = key

    def execute(self, context):
        hook = YoutubeApiHook(youtube_conn_id=self.youtube_conn_id)
        if not self.function_name:
            raise ValueError("function_name must be provided")
        method = getattr(hook, self.function_name)
        response = method(
            query=self.query, 
            max_results=self.max_results
        )

        aws_hook = S3Hook(aws_conn_id="aws_default")

        json_payload = json.dumps(response)
        if not self.bucket or not self.key:
            self.log.info("Bucket or key not provided — skipping S3 upload")
            return response

        # Use S3Hook helper to upload; avoids low-level hostname/SSL issues
        aws_hook.load_string(
            string_data=json_payload,
            key=self.key,
            bucket_name=self.bucket,
            replace=True
        )

        self.log.info(f"Data uploaded to s3://{self.bucket}/{self.key}")
        return response
# ...existing code...