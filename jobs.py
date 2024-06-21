import requests

# Replace with your Databricks instance URL
DATABRICKS_HOST = "https://<DATABRICKS_INSTANCE>.cloud.databricks.com"

# Replace with your Databricks user access token
# You can generate one from the Databricks UI -> User Settings -> Access Tokens
DATABRICKS_TOKEN = "dapi<your_access_token>"

# Replace with the job ID you want to get details for
JOB_ID = 123

# Construct the API endpoint URL
url = f"{DATABRICKS_HOST}/api/2.1/jobs/get?job_id={JOB_ID}"

# Set the authorization header
headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}

# Send the GET request
response = requests.get(url, headers=headers)

# Check for successful response
if response.status_code == 200:
  # Parse the JSON response
  job_details = response.json()
  print(f"Job Details for ID: {JOB_ID}")
  print(json.dumps(job_details, indent=2))  # Pretty print the JSON data
else:
  print(f"Error getting job details: {response.status_code}")
  print(response.text)
  
  
  import requests
import json
from datetime import datetime, timedelta

# Replace with your Databricks instance URL
DATABRICKS_HOST = "https://<DATABRICKS_INSTANCE>.cloud.databricks.com"

# Replace with your Databricks user access token
DATABRICKS_TOKEN = "dapi<your_access_token>"

# Set the time threshold for jobs (6 hours back from now)
now = datetime.utcnow()
six_hours_ago = now - timedelta(hours=6)

# Construct the API endpoint URL to list jobs
url = f"{DATABRICKS_HOST}/api/2.1/jobs/list"

# Set the authorization header
headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}

# Send the GET request
response = requests.get(url, headers=headers)

# Check for successful response
if response.status_code == 200:
  # Parse the JSON response
  jobs_list = response.json()
  print(f"Jobs run in the last 6 hours:")

  for job in jobs_list["jobs"]:
    # Check if job last ran within the time threshold
    last_run_time = datetime.fromisoformat(job["last_run_time"])
    if last_run_time >= six_hours_ago:
      # Get details for the job
      job_details_url = f"{DATABRICKS_HOST}/api/2.1/jobs/get?job_id={job['job_id']}"
      job_details_response = requests.get(job_details_url, headers=headers)
      if job_details_response.status_code == 200:
        job_details = job_details_response.json()
        print(f"\nJob ID: {job['job_id']}")
        print(json.dumps(job_details, indent=2))  # Pretty print the JSON data
      else:
        print(f"Error getting details for job {job['job_id']}")
else:
  print(f"Error getting job list: {response.status_code}")
  print(response.text)