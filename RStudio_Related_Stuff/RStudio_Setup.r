# Databricks notebook source
Sys.getenv("EXISTING_SPARKR_BACKEND_PORT")

# COMMAND ----------

DATABRICKS_GUID

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.fs.ls("/databricks/init/RStudio")

# COMMAND ----------

# MAGIC %sh head /dbfs/databricks/spark.R

# COMMAND ----------

# MAGIC %sh env

# COMMAND ----------

