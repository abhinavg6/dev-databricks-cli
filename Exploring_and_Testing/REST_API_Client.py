# Databricks notebook source
# MAGIC %md ## Prototype Job Creation REST Client

# COMMAND ----------

# MAGIC %md ### One-time admin activity
# MAGIC 
# MAGIC * Create a special API user and generate [personal access token](https://docs.databricks.com/api/latest/authentication.html) for it
# MAGIC * Login through the special API user, and add this prototype notebook to its workspace
# MAGIC * Edit the notebook to create appropriate widgets with default values, which could then be used in the JSON request body
# MAGIC * Install databricks-cli as a [PyPi library](https://docs.databricks.com/user-guide/libraries.html#creating-libraries) in special user's workspace
# MAGIC * Create a job for this notebook using Jobs UI
# MAGIC  * * Provide a appropriate Name
# MAGIC  * * Select Task as this Notebook
# MAGIC  * * Select Dependent Libraries as databricks-cli
# MAGIC  * * Edit Cluster as New Cluster with one driver node only (specify 0 worker nodes)
# MAGIC  * * Leave Schedule as None (as this will be a on-demand job)
# MAGIC  * * Configure Advanced-Alerts and provide email addresses of admin team
# MAGIC  * * Configure [Advanced-Permissions](https://docs.databricks.com/user-guide/jobs.html#jobs-access-control) for user team members as "Can Manage Run"
# MAGIC * Provide the job name and exposed parameters to user teams

# COMMAND ----------

# MAGIC %md ### Activity for each function-specific job - by user team members
# MAGIC 
# MAGIC * Create and test notebook interactively in own workspace
# MAGIC * If not done already, reach out to Admin team to provide "Can Manage Run" permissions for above On-Demand job
# MAGIC * Search On-Demand Job in [Jobs UI](https://docs.databricks.com/user-guide/jobs.html#viewing-jobs) and click it
# MAGIC * Press the button [Run Now With Different Parameters](https://docs.databricks.com/user-guide/jobs.html#running-a-notebook-job-with-different-parameters) to override values for exposed parameters, which would then be used to create the function-specific job
# MAGIC * Press the Run-specific link to see results - Job_Id might be useful to search the job in Jobs UI, if JOB_NAME wasn't overridden

# COMMAND ----------

import uuid

# Add widgets for  parameters that need to be exposed to users to be able to create scheduled jobs

dbutils.widgets.text("JOB_NAME", "job-" + str(uuid.uuid1()), "1. Job Name")
dbutils.widgets.text("NOTEBOOK_PATH", "/Users/abhinav.garg@databricks.com/Exploring_and_Testing/test-dbutils", "2. Notebook Path")
dbutils.widgets.text("NOTIFICATION_USERS", "abhinav.garg@databricks.com", "3. User Emails (Comma Sep)")
dbutils.widgets.text("TIMEOUT_SECONDS", "300", "4. Timeout (Seconds)")
dbutils.widgets.text("MAX_RETRIES", "2", "5. Max Retries")
dbutils.widgets.text("CRON_SCHEDULE", "0 * */2 * * ?", "6. Cron Schedule")

# COMMAND ----------

apiToken = dbutils.notebook.run("API_Token_Store", 60)

# COMMAND ----------

from databricks_cli.sdk import api_client
# Replace the shard name and token for special API user 
db_api = api_client.ApiClient(host = "https://field-eng.cloud.databricks.com/", token = apiToken)

# COMMAND ----------

print dbutils.widgets.get("JOB_NAME")
print dbutils.widgets.get("NOTEBOOK_PATH")
print dbutils.widgets.get("TIMEOUT_SECONDS")
print dbutils.widgets.get("MAX_RETRIES")

# COMMAND ----------

# Use appropriate widgets values while creating Job creation request
jobCreateResult = db_api.perform_query("POST", "/jobs/create", 
                                       {
                                         "name": dbutils.widgets.get("JOB_NAME"),
                                         "new_cluster": {
                                           "spark_version": "3.4.x-scala2.11",
                                           "node_type_id": "r3.xlarge",
                                           "aws_attributes": {
                                             "availability": "SPOT_WITH_FALLBACK",
                                             "first_on_demand": 3,
                                             "ebs_volume_type": "GENERAL_PURPOSE_SSD",
                                             "ebs_volume_count": 1,
                                             "ebs_volume_size": 100
                                           },
                                           "autoscale": {
                                             "min_workers": 2,
                                             "max_workers": 5
                                           },
                                           "custom_tags": [
                                             {"key": "Project", "value": "mck-cluster-145-2"}
                                           ]
                                         },
                                         "notebook_task": {
                                           "notebook_path": dbutils.widgets.get("NOTEBOOK_PATH")
                                         },
                                         "email_notifications": {
                                           "on_start": dbutils.widgets.get("NOTIFICATION_USERS").split(","),
                                           "on_success": dbutils.widgets.get("NOTIFICATION_USERS").split(","),
                                           "on_failure": dbutils.widgets.get("NOTIFICATION_USERS").split(",")
                                         },
                                         "timeout_seconds": int(dbutils.widgets.get("TIMEOUT_SECONDS")),
                                         "max_retries": int(dbutils.widgets.get("MAX_RETRIES")),
                                         "retry_on_timeout": True,
                                         "schedule": {
                                           "quartz_cron_expression": dbutils.widgets.get("CRON_SCHEDULE"),
                                           "timezone_id": "America/New_York"
                                         }
                                       })

# Print the job creation output so user can see the job_id generated (helpful if they haven't overridden JOB_NAME)
jobCreateResult

# COMMAND ----------

# MAGIC %md ### APPENDIX - Optional REST Queries for Request Data

# COMMAND ----------

versions = db_api.perform_query("GET", "/clusters/spark-versions")
versions

# COMMAND ----------

nodeTypes = db_api.perform_query("GET", "/clusters/list-node-types")
nodeTypes