from customHooks.DatabricksHook import DatabricksjobHook
from airflow.models import BaseOperator
import json

class DatabricksJobCreateOperator(BaseOperator):
    def __init__(
            self,
            job_name = None,
            task_key = None,
            notebook_path = None,
            *args,
            **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.job_name = job_name
        self.task_key = task_key
        self.notebook_path = notebook_path

    def execute(self, context):
        hook = DatabricksjobHook()
        job_config = {
            "name" : self.job_name,
            "tasks": [
            {
            "task_key": self.task_key,
            "notebook_task": {
                "notebook_path": self.notebook_path
            },
            "job_cluster_key": None,  # No shared cluster
            "compute": {
                "spec": {
                    "kind": "SERVERLESS"
                }
            }
            } 
            ],
            "email_notifications": {
            "on_start": ["bhavikabharti2@gmail.com"],
            "on_success": ["bhavikabharti2@gmail.com"],
            "on_failure": ["bhavikabharti2@gmail.com"]
            },
            "timeout_seconds": 3600,
            "max_concurrent_runs": 1
            }
        
        try:
            response_json = hook.create_job(job_config)  # Now a dict
            self.log.info("Databricks Job Created Successfully!")
            self.log.info(json.dumps(response_json, indent=2))

            job_id = response_json.get("job_id")
            return job_id
         # Airflow XCom safe
        except Exception as e:
            self.log.error("Failed to create job: %s", e)
            raise
    

        
