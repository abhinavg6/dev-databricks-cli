# Databricks notebook source
dbutils.fs.mkdirs("/mnt/databricks-abhinav/eu-soccer-events")
dbutils.fs.mkdirs("/mnt/databricks-abhinav/eu-soccer-events/input")
dbutils.fs.mkdirs("/mnt/databricks-abhinav/eu-soccer-events/interm")
dbutils.fs.mkdirs("/mnt/databricks-abhinav/eu-soccer-events/stream")
dbutils.fs.mkdirs("/mnt/databricks-abhinav/eu-soccer-events/checkpoint")

# COMMAND ----------

dbutils.fs.rm("/mnt/databricks-abhinav/eu-soccer-events/interm", True)
dbutils.fs.rm("/mnt/databricks-abhinav/eu-soccer-events/stream", True)
dbutils.fs.rm("/mnt/databricks-abhinav/eu-soccer-events/checkpoint", True)

# COMMAND ----------

# MAGIC %fs ls /mnt/databricks-abhinav/eu-soccer-events/input/

# COMMAND ----------

# MAGIC %sh head /dbfs/mnt/databricks-abhinav/eu-soccer-events/input/events.csv

# COMMAND ----------

eventsDf = (spark.read.csv("dbfs:/mnt/databricks-abhinav/eu-soccer-events/input/events.csv", 
                         inferSchema=True, header=True, 
                         ignoreLeadingWhiteSpace=True, 
                         ignoreTrailingWhiteSpace=True,
                         nullValue="NA"))

# COMMAND ----------

display(eventsDf)

# COMMAND ----------

(
  eventsDf.write.partitionBy("id_odsp").
                csv("dbfs:/mnt/databricks-abhinav/eu-soccer-events/interm", 
                    header=True, nullValue="NA", mode="overwrite")
)

# COMMAND ----------

eventsDf.dtypes

# COMMAND ----------

from pyspark.sql.types import *

schema = (StructType().
          add("id_odsp", StringType()).
          add("id_event", StringType()).
          add("sort_order", IntegerType()).
          add("time", IntegerType()).
          add("text", StringType()).
          add("event_type", IntegerType()).
          add("event_type2", IntegerType()).
          add("side", IntegerType()).
          add("event_team", StringType()).
          add("opponent", StringType()).
          add("player", StringType()).
          add("player2", StringType()).
          add("player_in", StringType()).
          add("player_out", StringType()).
          add("shot_place", IntegerType()).
          add("shot_outcome", IntegerType()).
          add("is_goal", IntegerType()).
          add("location", IntegerType()).
          add("bodypart", IntegerType()).
          add("assist_method", IntegerType()).
          add("situation", IntegerType()).
          add("fast_break", IntegerType())
         )

# COMMAND ----------

eventsStreamDf = (spark.readStream.
                  option("maxFilesPerTrigger", "5").
                  schema(schema).
                  csv("dbfs:/mnt/databricks-abhinav/eu-soccer-events/interm", 
                      header=True,
                      nullValue="NA"))

# COMMAND ----------

def mapKeyToVal(mapping):
    def mapKeyToVal_(col):
        return mapping.get(col)
    return udf(mapKeyToVal_, StringType())
  
evtTypeMap = {0:'Announcement', 1:'Attempt', 2:'Corner', 3:'Foul', 4:'Yellow card', 5:'Second yellow card', 6:'Red card', 7:'Substitution', 8:'Free kick won', 9:'Offside', 10:'Hand ball', 11:'Penalty conceded'}
evetTyp2Map = {12:'Key Pass', 13:'Failed through ball', 14:'Sending off', 15:'Own goal'}
sideMap = {1:'Home', 2:'Away'}

# COMMAND ----------

(
  eventsStreamDf.
  withColumn("event_type_str", mapKeyToVal(evtTypeMap)("event_type")).
  withColumn("event_type2_str", mapKeyToVal(evetTyp2Map)("event_type2")).
  withColumn("side_str", mapKeyToVal(sideMap)("side"))
)

# COMMAND ----------

(
  eventsStreamDf
  .writeStream
  .format("parquet")
  .option("path", "dbfs:/mnt/databricks-abhinav/eu-soccer-events/stream")
  .partitionBy("id_odsp")
  .trigger(processingTime='10 seconds')
  .option("checkpointLocation", "dbfs:/mnt/databricks-abhinav/eu-soccer-events/checkpoint")
  .start()
)

# COMMAND ----------

gameInfoDf = (spark.read.csv("dbfs:/mnt/databricks-abhinav/eu-soccer-events/input/ginf.csv", 
                         inferSchema=True, header=True, 
                         ignoreLeadingWhiteSpace=True, 
                         ignoreTrailingWhiteSpace=True,
                         nullValue="NA"))

# COMMAND ----------

display(gameInfoDf)

# COMMAND ----------

