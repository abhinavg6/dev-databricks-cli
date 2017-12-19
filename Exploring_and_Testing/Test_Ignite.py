# Databricks notebook source
# MAGIC %scala
# MAGIC import org.apache.ignite.spark.{IgniteContext, IgniteRDD}
# MAGIC import org.apache.ignite.configuration.IgniteConfiguration
# MAGIC 
# MAGIC val igniteContext = new IgniteContext(sc, () => new IgniteConfiguration())

# COMMAND ----------

