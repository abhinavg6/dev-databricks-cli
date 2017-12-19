# Databricks notebook source
dbutils.fs.ls("s3a://databricks-abhinav-fe-mck-test")

# COMMAND ----------

dbutils.fs.cp("/FileStore/test.tif", "s3a://databricks-abhinav-fe-mck-test/")

# COMMAND ----------

dbutils.fs.mount("s3a://databricks-abhinav-fe-mck-test", "/mnt/db-abhi-mck-test", "sse-kms")

# COMMAND ----------

dbutils.fs.cp("/FileStore/test.tif", "/mnt/db-abhi-mck-test/")

# COMMAND ----------

import boto3
from botocore.client import Config

client = boto3.client('s3', config=Config(signature_version='s3v4'))
client.put_object(Body=open('/dbfs/FileStore/test.tif', 'rb'), Bucket='databricks-abhinav-fe-mck-test', Key='test.tif', ServerSideEncryption='aws:kms')

# COMMAND ----------

client.list_objects(Bucket='databricks-abhinav-fe-mck-test')

# COMMAND ----------

