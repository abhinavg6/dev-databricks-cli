// Databricks notebook source
//dbutils.fs.mount("s3a://databricks-field-eng-abhinavgarg", "/mnt/databricks-abhinav")

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/mnt/databricks-abhinav/bitcoin-data"))

// COMMAND ----------

// MAGIC %fs head dbfs:/mnt/databricks-abhinav/bitcoin-data/krakenUSD_1-min_data_2014-01-07_to_2017-05-31.csv

// COMMAND ----------

// MAGIC %python
// MAGIC bitcoinDf = spark.read.csv(path='dbfs:/mnt/databricks-abhinav/bitcoin-data', header=True, inferSchema=True)

// COMMAND ----------

// MAGIC %python
// MAGIC bitcoinDf.rdd.getNumPartitions()

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import *
// MAGIC filteredBitcoinDf = bitcoinDf.dropna(how='all', subset=['Open','High','Low','Close'])
// MAGIC 
// MAGIC # Transform unix epoch time to timestamp type
// MAGIC filteredBitcoinDf = filteredBitcoinDf.withColumn('TimestampStr', to_timestamp(from_unixtime(filteredBitcoinDf.Timestamp))).drop(filteredBitcoinDf.Timestamp).withColumnRenamed('TimestampStr','Timestamp')
// MAGIC 
// MAGIC # Rename columns to remove special chars
// MAGIC filteredBitcoinDf = filteredBitcoinDf.withColumnRenamed('Volume_(BTC)','Volume_BTC').withColumnRenamed('Volume_(Currency)','Volume_Currency')
// MAGIC 
// MAGIC # Add Tear and Month columns
// MAGIC filteredBitcoinDf = filteredBitcoinDf.withColumn('Year', year(filteredBitcoinDf.Timestamp)).withColumn('Month', month(filteredBitcoinDf.Timestamp)).withColumn('DayOfMonth', dayofmonth(filteredBitcoinDf.Timestamp))
// MAGIC display(filteredBitcoinDf)

// COMMAND ----------

// MAGIC %fs ls tmp/

// COMMAND ----------

// MAGIC %fs ls dbfs:/user/hive/warehouse/dskip/	

// COMMAND ----------

// MAGIC %sql CREATE DATABASE IF NOT EXISTS bitcoin

// COMMAND ----------

// MAGIC %sql USE bitcoin

// COMMAND ----------

// MAGIC %python 
// MAGIC filteredBitcoinDf.write.saveAsTable('bitcoin_transformed', format='parquet', mode='overwrite', partitionBy=['Year','Month'])

// COMMAND ----------

// MAGIC %sql select DayOfMonth, Month, avg(Open), avg(High) 
// MAGIC from bitcoin_transformed 
// MAGIC where Year = 2016 
// MAGIC group by DayOfMonth, Month 
// MAGIC order by DayOfMonth, Month

// COMMAND ----------

