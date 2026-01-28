import requests
import json
from airflow.hooks.base import BaseHook

class DatabricksjobHook(BaseHook):
    DATABRICKS_INSTANCE = "https://dbc-xxxxxxxx-cefb.cloud.databricks.com"
    BASE_URL = f"{DATABRICKS_INSTANCE}/api/2.1"

    def __init__(self, databricks_conn_id="databricks_default"):
        super().__init__()
        self.databricks_conn_id = databricks_conn_id

    def get_token(self):
        conn = self.get_connection(self.databricks_conn_id)
        return conn.password
    
    def _request(self, endpoint, method='POST', job_payload=None):
        token = self.get_token()
        print("Using Databricks Token:", token)
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        url = f"{self.BASE_URL}/{endpoint}"
        if method == 'POST':
            print("Job Payload:", job_payload)
            response = requests.post(url, headers=headers, data=json.dumps(job_payload))
        elif method == 'GET':
            response = requests.get(url, headers=headers, params=None)
        else:
            raise ValueError("Unsupported HTTP method")
        
        response.raise_for_status()
        return response.json()
    
    def create_job(self, job_config):
        return self._request('jobs/create', method='POST', job_payload=job_config)
    
    def get_jobslist(self):
        job_ids = []
        json = self._request('jobs/list', method='GET')
        for job in json.get('jobs', []):
            job_ids.append(job['job_id'])
        print("Fetched Job IDs:", job_ids)
        return job_ids

    
    def run_job(self, job_ids, notebook_params=None):
        print("Running jobs with IDs:", job_ids)

        if isinstance(job_ids, str):
            try:
                job_ids = json.loads(job_ids)
            except:
                job_ids = [int(job_ids)]

        elif isinstance(job_ids, int):
            job_ids = [job_ids]

        for job_id in job_ids:
            payload = {
                "job_id": int(job_id),
                "notebook_params": notebook_params or {},
            }
            print("Payload:", payload)
            self._request('jobs/run-now', method='POST', job_payload=payload)

        return {"status": "Jobs triggered successfully"}
