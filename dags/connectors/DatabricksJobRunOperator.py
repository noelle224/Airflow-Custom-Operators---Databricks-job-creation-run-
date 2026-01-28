from customHooks.DatabricksHook import DatabricksjobHook
from airflow.models import BaseOperator
import json

class DatabricksJobRunOperator(BaseOperator):
    template_fields = ("job_ids",) 
    def __init__(
            self,
            job_ids = None,
            notebook_params = None,
            *args,
            **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.job_ids = job_ids
        self.notebook_params = notebook_params

    def execute(self, context):
        job_ids = self.job_ids  # now rendered value
        self.log.info(f"Running Databricks job_id: {job_ids}")
        
        hook = DatabricksjobHook()
        try:
            response_json = hook.run_job(self.job_ids, self.notebook_params)
            self.log.info("Databricks Job Run Successfully!")
            self.log.info(json.dumps(response_json, indent=2))

            run_id = response_json.get("run_id")
            return run_id
         # Airflow XCom safe
        except Exception as e:
            self.log.error("Failed to run job: %s", e)
            raise
        