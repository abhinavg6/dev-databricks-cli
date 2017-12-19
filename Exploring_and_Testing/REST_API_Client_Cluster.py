# Databricks notebook source
# MAGIC %md ## Prototype Cluster Creation REST Client

# COMMAND ----------

# MAGIC %md This notebook will do the following:
# MAGIC 
# MAGIC * Accept parameters using widgets (via job parameters if job is created)
# MAGIC * Create the cluster name as finra-{CLUSTER_NAME}
# MAGIC * If the cluster with name already exists and is in TERMINATED state, then start it (otherwise do nothing if it exists)
# MAGIC * If the cluster doesn't exist:
# MAGIC * * If old cluster config exists and it need not be overridden (default), then get it and create the cluster
# MAGIC * * If old cluster config doesn't exist or it needs to be overridden, then create cluster with new config

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
# MAGIC  * * If needed, configure [Advanced-Permissions](https://docs.databricks.com/user-guide/jobs.html#jobs-access-control) for user team members as "Can Manage Run"
# MAGIC * If needed, provide the job name and exposed parameters to user teams

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

import uuid

# Add widgets for  parameters that need to be exposed to users to be able to create scheduled jobs

dbutils.widgets.text("CLUSTER_NAME", "dataeng-abc", "1. Cluster Name")
dbutils.widgets.text("SPARK_VERSION", "3.4.x-scala2.11", "2. Spark Version")
dbutils.widgets.text("NODE_TYPE", "i3.xlarge", "3. Node Type")
dbutils.widgets.text("MIN_WORKERS", "2", "4. Min. Workers")
dbutils.widgets.text("MAX_WORKERS", "5", "5. Max. Workers")
dbutils.widgets.text("ON_DEMAND", "3", "6. On Demand (Incl. Driver)")
dbutils.widgets.text("OVERRIDE_CONFIG", "False", "7. Override Config (True/False)")
dbutils.widgets.text("PROJECT_TAG", "DATA-ENG-TEAM", "8. Tag-Project")

# COMMAND ----------

print dbutils.widgets.get("CLUSTER_NAME")
print dbutils.widgets.get("SPARK_VERSION")
print dbutils.widgets.get("NODE_TYPE")
print dbutils.widgets.get("MIN_WORKERS")
print dbutils.widgets.get("MAX_WORKERS")
print dbutils.widgets.get("OVERRIDE_CONFIG")
print dbutils.widgets.get("PROJECT_TAG")

# COMMAND ----------

apiToken = dbutils.notebook.run("API_Token_Store", 60)

# COMMAND ----------

import requests
from requests.auth import HTTPBasicAuth

reqHeaders = {'Authorization': 'Bearer ' + apiToken}
#response = requests.post('https://field-eng.cloud.databricks.com/j_security_check', headers=reqHeaders)
response = requests.post('https://field-eng.cloud.databricks.com/j_security_check', auth=HTTPBasicAuth('abhinav.garg+api@databricks.com', 'apiUser1#'))
print response.content
print response.headers

# COMMAND ----------

reqHeaders = {'Authorization': 'Bearer ' + apiToken, 'Cookie': 'JSESSIONID=webapp-ip-10-119-244-19839lx6b5i5hj21w9iqmoxsqmve.webapp-ip-10-119-244-198'}
response = requests.get('https://field-eng.cloud.databricks.com/config ', headers=reqHeaders)
print response.content
print response.headers

# COMMAND ----------

#from databricks_cli.sdk import api_client
# Replace the shard name and token for special API user
db_api = api_client.ApiClient(host = "https://field-eng.cloud.databricks.com/", token = apiToken)

# COMMAND ----------

import json
import os

clusterName = "finra-" + dbutils.widgets.get("CLUSTER_NAME")
existingClusters = db_api.perform_query("GET", "/clusters/list")
shouldExit = False
for existingCluster in existingClusters['clusters']:
  if existingCluster['cluster_name'] == clusterName:
    print "Cluster exists in " + existingCluster['state'] + " state"
    # Start the existing cluster if it exists and is in terminated state
    if existingCluster['state'] == 'TERMINATED':
      db_api.perform_query("POST", "/clusters/start", { "cluster_id": existingCluster['cluster_id'] })
    # Don't need to create cluster if it already exists
    shouldExit = True
    break

# COMMAND ----------

dbutils.fs.mkdirs("/FileStore/finra-clusters/")

# COMMAND ----------

if not shouldExit:
  # Check if we need to reuse old/existing cluster config
  if (os.path.isfile('/dbfs/FileStore/finra-clusters/' + clusterName + '.json') 
        and dbutils.widgets.get("OVERRIDE_CONFIG") == 'False'):
    print "Cluster config exists and we need to reuse"
    clusterJson = json.load(open('/dbfs/FileStore/finra-clusters/' + clusterName + '.json'))
  else:
    print "Need to create new cluster config"
    # Create cluster request JSON
    clusterJson = {
                    "cluster_name": clusterName,
                    "spark_version": dbutils.widgets.get("SPARK_VERSION"),
                    "node_type_id": dbutils.widgets.get("NODE_TYPE"),
                    "aws_attributes": {
                      "availability": "SPOT_WITH_FALLBACK",
                      "first_on_demand": int(dbutils.widgets.get("ON_DEMAND")),
                      "ebs_volume_type": "GENERAL_PURPOSE_SSD",
                      "ebs_volume_count": 1,
                      "ebs_volume_size": 100
                    },
                    "autoscale": {
                      "min_workers": int(dbutils.widgets.get("MIN_WORKERS")),
                      "max_workers": int(dbutils.widgets.get("MAX_WORKERS"))
                    },
                    "custom_tags": [
                      {"key": "Project", "value": dbutils.widgets.get("PROJECT_TAG")}
                    ]
                  }

    # Force remove any existing config if it exists
    dbutils.fs.rm("/FileStore/finra-clusters/" + clusterName + ".json")
    # Write the latest cluster config and store for later reuse
    with open('/dbfs/FileStore/finra-clusters/' + clusterName + '.json', 'w') as outfile:
      json.dump(clusterJson, outfile)
    print "Written latest cluster config for later reuse"
      
  print "Cluster JSON is " + json.dumps(clusterJson)
  print "Create new cluster"
  clusterCreateResult = db_api.perform_query("POST", "/clusters/create", clusterJson)
  print "Cluster create result is " + json.dumps(clusterCreateResult)
else:
  print "Cluster already exists, do nothing"

# COMMAND ----------

