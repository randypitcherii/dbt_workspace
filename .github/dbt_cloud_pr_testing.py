import requests
import os

api_base = os.getenv('DBT_CLOUD_URL', 'https://cloud.getdbt.com') # default to multitenant url
api_key = os.environ['DBT_CLOUD_API_KEY']  # no default here, just throw an error here if key not provided
account_id = os.environ['DBT_CLOUD_ACCOUNT_ID'] # no default here, just throw an error here if id not provided
job_id = os.environ['DBT_CLOUD_PR_JOB_ID'] # no default here, just throw an error here if id not provided

# run job
headers = {'Authorization': f'Token {api_key}'}
run_job_url = f'{api_base}/api/v2/accounts/{account_id}/jobs/{job_id}/run/'
payload = {'cause': 'Triggered from python workflow'} # optionally, specify a branch to run here
run_job_resp = requests.post(run_job_url, headers=headers, data=payload)


