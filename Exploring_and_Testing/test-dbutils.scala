// Databricks notebook source
val notebookContextMap = dbutils.notebook.getContext.toMap.get("tags").get.asInstanceOf[Map[String,String]]

// COMMAND ----------

val notebookMap = dbutils.notebook.getContext.toMap

// COMMAND ----------

spark.conf.get("spark.sql.cbo.enabled")

// COMMAND ----------

