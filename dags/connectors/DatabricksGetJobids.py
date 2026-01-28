from customHooks.DatabricksHook import DatabricksjobHook
from airflow.models import BaseOperator
import json

class DatabricksGetJobids(BaseOperator):
    def __init__(
            self,
            *args,
            **kwargs
    ):
        super().__init__(*args, **kwargs)

    def execute(self, context):
        hook = DatabricksjobHook()
        try:
            job_ids = hook.get_jobslist()
            self.log.info("job_ids fetched!")
            return job_ids
         # Airflow XCom safe
        except Exception as e:
            self.log.error("Failed to get jobs: %s", e)
            raise