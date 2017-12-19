# Databricks notebook source
# MAGIC %fs rm s3a://databricks-field-eng-audit-logs/databricks/field-eng/audit-logs/_databricks_dev

# COMMAND ----------

# MAGIC %fs ls s3a://databricks-field-eng-audit-logs/databricks/field-eng/audit-logs/

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE IF EXISTS fieldEngAuditLogs;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS fieldEngAuditLogs
# MAGIC     USING json
# MAGIC     OPTIONS (
# MAGIC       path "s3a://databricks-field-eng-audit-logs/databricks/field-eng/audit-logs/"
# MAGIC     );
# MAGIC     
# MAGIC MSCK REPAIR TABLE fieldEngAuditLogs;

# COMMAND ----------

# MAGIC %sql describe table fieldEngAuditLogs

# COMMAND ----------

# MAGIC %sql select * from fieldEngAuditLogs order by date desc

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select serviceName, actionName, count(*) as cnt 
# MAGIC   from fieldEngAuditLogs group by serviceName, actionName 
# MAGIC   order by serviceName, cnt desc

# COMMAND ----------

# MAGIC %md ### Cluster actions

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select userIdentity.email, count(*) as cnt
# MAGIC from fieldEngAuditLogs 
# MAGIC where serviceName="clusters" and actionName="create"
# MAGIC group by userIdentity.email
# MAGIC order by cnt desc

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select requestParams.cluster_creator, count(*)
# MAGIC from fieldEngAuditLogs 
# MAGIC where serviceName="clusters" and actionName="create" and userIdentity.email="Unknown"
# MAGIC group by requestParams.cluster_creator

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select userIdentity, requestParams, response 
# MAGIC   from fieldEngAuditLogs 
# MAGIC   where serviceName="clusters" 
# MAGIC   and actionName="create"
# MAGIC   and !(userIdentity.email="Unknown" and requestParams.cluster_creator="JOB_LAUNCHER")

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.functions import *
# MAGIC from pyspark.sql.types import *
# MAGIC 
# MAGIC awsdf = spark.sql("select userIdentity, requestParams.aws_attributes, date from fieldEngAuditLogs where actionName=\"create\" and serviceName=\"clusters\"")
# MAGIC 
# MAGIC schema = StructType().add("availability", StringType()).add("ebs_volume_count", IntegerType()).add("ebs_volume_type", StringType()).add("ebs_volume_size", IntegerType())
# MAGIC parsedAwsDf = (awsdf.select("userIdentity", 
# MAGIC                             "date", 
# MAGIC                             from_json("aws_attributes", schema).alias("awsAttr")).
# MAGIC                groupBy('userIdentity.email', 
# MAGIC                        'awsAttr.ebs_volume_type').
# MAGIC                agg(sum(col('awsAttr.ebs_volume_count')).alias('tot_ebs_vols'), 
# MAGIC                    sum(col('awsAttr.ebs_volume_size')).alias('tot_ebs_vol_size'))
# MAGIC               )
# MAGIC 
# MAGIC display(parsedAwsDf.filter(parsedAwsDf.ebs_volume_type.isNotNull()).sort("tot_ebs_vols", ascending=False))
# MAGIC #display(parsedAwsDf.filter(parsedAwsDf.ebs_volume_type.isNotNull()).sort("date", ascending=False))

# COMMAND ----------

# MAGIC %md ## Job actions

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select requestParams.jobId, count(*) as cnt
# MAGIC   from fieldEngAuditLogs 
# MAGIC   where serviceName="jobs" and actionName='runFailed'
# MAGIC   group by requestParams.jobId
# MAGIC   order by cnt desc

# COMMAND ----------

# MAGIC %sql SET spark.sql.crossJoin.enabled=true

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select distinct job_tmp.jobId, cluster_tmp.node_type_id, cluster_tmp.spark_version
# MAGIC from
# MAGIC (
# MAGIC   select requestParams.jobId, requestParams.idInJob
# MAGIC   from fieldEngAuditLogs 
# MAGIC   where serviceName="jobs" and actionName='runFailed'
# MAGIC ) job_tmp 
# MAGIC   join
# MAGIC (
# MAGIC   select requestParams.cluster_name, requestParams.node_type_id, requestParams.spark_version
# MAGIC   from fieldEngAuditLogs 
# MAGIC   where serviceName="clusters" and actionName="create" and requestParams.cluster_creator="JOB_LAUNCHER"
# MAGIC ) cluster_tmp
# MAGIC on ('job-' || CAST(job_tmp.jobId AS STRING) || '-run-' || CAST(job_tmp.idInJob AS STRING)) = cluster_tmp.cluster_name

# COMMAND ----------

# MAGIC %md ## Others - Accounts, SQL ACLs, Notebooks

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct userIdentity.email, date, requestParams.user, response
# MAGIC from fieldEngAuditLogs
# MAGIC where serviceName = "accounts" and actionName = "login" and response.statusCode != 200
# MAGIC order by date desc

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select userIdentity.email, requestParams.permission, date
# MAGIC from fieldEngAuditLogs
# MAGIC where serviceName = "sqlPermissions" and actionName = 'grantPermission' 
# MAGIC order by date desc

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select cluster_tmp.clusterName, count(*) as cnt
# MAGIC from
# MAGIC (
# MAGIC   select distinct requestParams.clusterId, requestParams.notebookId
# MAGIC   from fieldEngAuditLogs
# MAGIC   where serviceName = "notebook" and actionName = 'attachNotebook'
# MAGIC ) nb_tmp
# MAGIC   inner join
# MAGIC (
# MAGIC   select requestParams.clusterId, requestParams.clusterName
# MAGIC   from fieldEngAuditLogs
# MAGIC   where serviceName = "clusters" and actionName = 'createResult'
# MAGIC ) cluster_tmp
# MAGIC on nb_tmp.clusterId = cluster_tmp.clusterId
# MAGIC group by clusterName
# MAGIC order by cnt desc

# COMMAND ----------

