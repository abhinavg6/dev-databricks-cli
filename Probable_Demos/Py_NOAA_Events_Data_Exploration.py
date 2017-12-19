# Databricks notebook source
# MAGIC %md
# MAGIC ![Databricks for Weather Analytics](https://changingwinds.files.wordpress.com/2017/05/storm-clouds.jpg?w=600)
# MAGIC 
# MAGIC # NOAA Storm Events Data Analysis
# MAGIC * Storms and other severe weather events can cause both public health and economic problems for communities and municipalities. Many severe events can result in fatalities, injuries, and property damage. U.S. National Oceanic and Atmospheric Administration’s (NOAA) keeps a database, which tracks characteristics of major storms and weather events in the United States, including when and where they occur, as well as estimates of any fatalities, injuries, and property damage. 
# MAGIC * In this report, we explore that data and answer questions like:
# MAGIC   * What weather events have had most adverse impact on population health and economy over the years?
# MAGIC   * Which states have incurred maximum population and economic damage over the years?
# MAGIC   * In a particular state, what weather events have had maximum adverse impact each year, for last 10 years?
# MAGIC * Such weather data analysis is applicable in different verticals - Financial Services, Aerospace, Automotive, Healthcare, Insurance etc., when combined with internal data and other external datasets of interest

# COMMAND ----------

# MAGIC %md ##1. Data Sourcing/Extraction
# MAGIC 
# MAGIC We will download the data hosted at [**NOAA Site**](ftp://ftp.ncdc.noaa.gov/pub/data/swdi/stormevents/csvfiles/). There're two datasets of interest:
# MAGIC 
# MAGIC * Storm Event Details, with important fields as:
# MAGIC   * EPISODE_ID - ID assigned by NWS to denote the storm episode
# MAGIC   * EVENT_ID - ID assigned by NWS to note a single, small part that goes into a specific storm episode
# MAGIC   * STATE - The state name where the event occurred
# MAGIC   * YEAR - Four digit year for the event in this record
# MAGIC   * MONTH_NAME - Name of the month for the event in this record
# MAGIC   * EVENT_TYPE - Actual event type being recorded, e.g. Hail, Thunderstorm Wind, Snow, Ice etc.
# MAGIC   * CZ_NAME - County/Parish, Zone or Marine Name assigned to the county FIPS number or NWS Forecast Zone
# MAGIC   * CZ_TYPE - Indicates whether the event happened in a (C) county/parish, (Z) zone or (M) marine
# MAGIC   * INJURIES_DIRECT - The number of injuries directly related to the weather event
# MAGIC   * INJURIES_INDIRECT - The number of injuries indirectly related to the weather event
# MAGIC   * DEATHS_DIRECT - The number of deaths directly related to the weather event
# MAGIC   * DEATHS_INDIRECT - The number of deaths indirectly related to the weather event
# MAGIC   * DAMAGE_PROPERTY - The estimated amount of damage to property incurred by the weather event
# MAGIC   * DAMAGE_CROPS - The estimated amount of damage to crops incurred by the weather event 
# MAGIC * Storm Fatalities Data, with important fields as:
# MAGIC   * FATALITY_ID - ID assigned by NWS to denote the individual fatality that occurred within a storm event
# MAGIC   * EVENT_ID - ID assigned by NWS to note a single, small part that goes into a specific storm episode; links with above dataset
# MAGIC   * FATALITY_TYPE - Direct or Indirect Fatality
# MAGIC   * FATALITY_AGE - Age of the fatality
# MAGIC   * FATALITY_SEX - Gender of the fatality
# MAGIC   * FATALITY_LOCATION - E.g. Under Tree, Boating, Vehicle/Towed Trailer etc.

# COMMAND ----------

# MAGIC %md * Recreate the source data folders

# COMMAND ----------

dbutils.fs.rm("/mnt/databricks-abhinav/noaa-storm", True)

# COMMAND ----------

dbutils.fs.mkdirs("/mnt/databricks-abhinav/noaa-storm")
dbutils.fs.mkdirs("/mnt/databricks-abhinav/noaa-storm/input/events")
dbutils.fs.mkdirs("/mnt/databricks-abhinav/noaa-storm/input/fatalities")

# COMMAND ----------

# MAGIC %md Source data files for all years:
# MAGIC * Download all files of interest
# MAGIC * Unzip the files
# MAGIC * Move unzipped files to source data folders
# MAGIC * Verify

# COMMAND ----------

# MAGIC %sh 
# MAGIC 
# MAGIC mkdir /tmp/noaa-storm
# MAGIC 
# MAGIC # Get all relevant event files from NOAA FTP server
# MAGIC wget -r --no-parent -P /tmp/noaa-storm -A 'StormEvents_details-ftp_v1.0*.csv.gz' ftp://ftp.ncdc.noaa.gov/pub/data/swdi/stormevents/csvfiles/
# MAGIC # Unzip all the downloaded event files
# MAGIC gunzip -v /tmp/noaa-storm/ftp.ncdc.noaa.gov/pub/data/swdi/stormevents/csvfiles/*.csv.gz
# MAGIC # Move all unzipped event files to mounted S3 folder
# MAGIC mv -v /tmp/noaa-storm/ftp.ncdc.noaa.gov/pub/data/swdi/stormevents/csvfiles/*.csv /dbfs/mnt/databricks-abhinav/noaa-storm/input/events
# MAGIC 
# MAGIC rm -rf /tmp/noaa-storm

# COMMAND ----------

# MAGIC %sh 
# MAGIC 
# MAGIC mkdir /tmp/noaa-storm
# MAGIC 
# MAGIC # Get all relevant fatalities files from NOAA FTP server
# MAGIC wget -r --no-parent -P /tmp/noaa-storm -A 'StormEvents_fatalities-ftp_v1.0*.csv.gz' ftp://ftp.ncdc.noaa.gov/pub/data/swdi/stormevents/csvfiles/
# MAGIC # Unzip all the downloaded fatalities files
# MAGIC gunzip -v /tmp/noaa-storm/ftp.ncdc.noaa.gov/pub/data/swdi/stormevents/csvfiles/*.csv.gz
# MAGIC # Move all unzipped fatalities files to mounted S3 folder
# MAGIC mv -v /tmp/noaa-storm/ftp.ncdc.noaa.gov/pub/data/swdi/stormevents/csvfiles/*.csv /dbfs/mnt/databricks-abhinav/noaa-storm/input/fatalities
# MAGIC 
# MAGIC rm -rf /tmp/noaa-storm

# COMMAND ----------

# MAGIC %fs ls /mnt/databricks-abhinav/noaa-storm/input/events

# COMMAND ----------

# MAGIC %fs ls /mnt/databricks-abhinav/noaa-storm/input/fatalities

# COMMAND ----------

# MAGIC %md * Read the Storm Event Details dataset, and create a synthetic dataset mapping state names to state codes

# COMMAND ----------

stormDf = (spark.read.csv("dbfs:/mnt/databricks-abhinav/noaa-storm/input/events", 
                         inferSchema=True, header=True, 
                         ignoreLeadingWhiteSpace=True, 
                         ignoreTrailingWhiteSpace=True))

statesAndCodes = [('ALABAMA','AL'), ('ALASKA','AK'), ('AMERICAN SAMOA','AS'), ('ARIZONA','AZ'),
                 ('ARKANSAS','AR'), ('CALIFORNIA','CA'), ('COLORADO','CO'), ('CONNECTICUT','CT'),
                 ('DELAWARE','DE'), ('DISTRICT OF COLUMBIA','DC'), ('FLORIDA','FL'), ('GEORGIA','GA'),
                 ('GUAM','GU'), ('HAWAII','HI'), ('HAWAII WATERS','HI'), ('IDAHO','ID'), ('ILLINOIS','IL'),
                 ('INDIANA','IN'), ('IOWA','IA'), ('KANSAS','KS'), ('KENTUCKY','KY'), ('LOUISIANA','LA'),
                 ('MAINE','ME'), ('MARYLAND','MD'), ('MASSACHUSETTS','MA'), ('MICHIGAN','MI'),
                 ('MINNESOTA','MN'), ('MISSISSIPPI','MS'), ('MISSOURI','MO'), ('MONTANA','MT'),
                 ('NEBRASKA','NE'), ('NEVADA','NV'), ('NEW HAMPSHIRE','NH'), ('NEW JERSEY','NJ'),
                 ('NEW MEXICO','NM'), ('NEW YORK','NY'), ('NORTH CAROLINA','NC'), ('NORTH DAKOTA','ND'),
                 ('OHIO','OH'), ('OKLAHOMA','OK'), ('OREGON','OR'), ('PENNSYLVANIA','PA'),
                 ('PUERTO RICO','PR'), ('RHODE ISLAND','RI'), ('SOUTH CAROLINA','SC'),
                 ('SOUTH DAKOTA','SD'), ('TENNESSEE','TN'), ('TEXAS','TX'), ('UTAH','UT'),
                 ('VERMONT','VT'), ('VIRGIN ISLANDS','VI'), ('VIRGINIA','VA'), ('WASHINGTON','WA'),
                 ('WEST VIRGINIA','WV'), ('WISCONSIN','WI'), ('WYOMING','WY')]
statesAndCodesDf = spark.createDataFrame(statesAndCodes, ['STATE_NAME', 'STATE_CODE'])

# COMMAND ----------

display(stormDf)

# COMMAND ----------

# MAGIC %md ## 2. Data Transformation
# MAGIC Convert the data to a format, such that one could gather meaningful insights from it

# COMMAND ----------

# MAGIC %md * Join the storm event details dataset with our synthetic dataset

# COMMAND ----------

from pyspark.sql.functions import *

joinedStormDf = (stormDf.filter(stormDf['STATE'].isNotNull()).
                 join(statesAndCodesDf, trim(stormDf['STATE']) == statesAndCodesDf['STATE_NAME']))

# COMMAND ----------

# MAGIC %md 
# MAGIC * Merge similar looking event types
# MAGIC * Create new columns depicting total injuries (Direct and Indirect) and total deaths (Direct and Indirect) per event
# MAGIC * Fill empty/null economic damage data with 0.00K (K indicating thousands)
# MAGIC * Project fields of interest to make the dataset leaner

# COMMAND ----------

transformedStormDf = (joinedStormDf.
                      withColumn('EVENT_TYPE', 
                                 when(joinedStormDf['EVENT_TYPE'].rlike('(?i)Thunderstorm Wind.*'), 'THUNDERSTORM WIND').
                                 when(joinedStormDf['EVENT_TYPE'].rlike('(?i)Tornado.*'), 'TORNADO').
                                 otherwise(upper(joinedStormDf['EVENT_TYPE']))).
                      withColumn('TOT_INJURIES', 
                                 joinedStormDf['INJURIES_DIRECT'] + joinedStormDf['INJURIES_INDIRECT']).
                      withColumn('TOT_DEATHS', 
                                 joinedStormDf['DEATHS_DIRECT'] + joinedStormDf['DEATHS_INDIRECT']).
                      withColumn('DAMAGE_CROPS', 
                                 when(joinedStormDf['DAMAGE_CROPS'].isNull(), '0.00K').
                                 otherwise(joinedStormDf['DAMAGE_CROPS'])).
                      withColumn('DAMAGE_PROPERTY', 
                                 when(joinedStormDf['DAMAGE_PROPERTY'].isNull(), '0.00K').
                                 otherwise(joinedStormDf['DAMAGE_PROPERTY']))
                     )

relevantCols = ["EPISODE_ID","EVENT_ID","STATE_CODE","STATE_NAME","YEAR","MONTH_NAME",
                "EVENT_TYPE","CZ_TYPE","CZ_NAME","TOT_INJURIES","TOT_DEATHS",
                "DAMAGE_PROPERTY","DAMAGE_CROPS",
                "EPISODE_NARRATIVE","EVENT_NARRATIVE"]

transformedStormDf = transformedStormDf.select(relevantCols)

# COMMAND ----------

# MAGIC %md
# MAGIC * Normalize economic damage data to thousands ($)
# MAGIC * Create a new column depicting total economic damage (Property and Crops)

# COMMAND ----------

transformedStormDf2 = (transformedStormDf.
                       withColumn('DAMAGE_PROPERTY', 
                                  when(transformedStormDf['DAMAGE_PROPERTY'].endswith('K'), 
                                       regexp_replace(transformedStormDf['DAMAGE_PROPERTY'],'K','').cast('double')).
                                  when(transformedStormDf['DAMAGE_PROPERTY'].endswith('M'),
                                       regexp_replace(transformedStormDf['DAMAGE_PROPERTY'],'M','').cast('double') * 1000).
                                  when(transformedStormDf['DAMAGE_PROPERTY'].endswith('B'),
                                       regexp_replace(transformedStormDf['DAMAGE_PROPERTY'],'B','').cast('double') * 1000000)
                                 ).
                       withColumnRenamed('DAMAGE_PROPERTY', 'DAMAGE_PROPERTY_THDS').
                       withColumn('DAMAGE_CROPS', 
                                  when(transformedStormDf['DAMAGE_CROPS'].endswith('K'), 
                                       regexp_replace(transformedStormDf['DAMAGE_CROPS'],'K','').cast('double')).
                                  when(transformedStormDf['DAMAGE_CROPS'].endswith('M'),
                                       regexp_replace(transformedStormDf['DAMAGE_CROPS'],'M','').cast('double') * 1000).
                                  when(transformedStormDf['DAMAGE_CROPS'].endswith('B'),
                                       regexp_replace(transformedStormDf['DAMAGE_CROPS'],'B','').cast('double') * 1000000)
                                 ).
                       withColumnRenamed('DAMAGE_CROPS', 'DAMAGE_CROPS_THDS')
                      )

transformedStormDf2 = transformedStormDf2.withColumn('TOT_DAMAGE_THDS', 
                                                     transformedStormDf2['DAMAGE_PROPERTY_THDS'] + transformedStormDf2['DAMAGE_CROPS_THDS'])

display(transformedStormDf2)

# COMMAND ----------

# MAGIC %md ## 3. Data Loading
# MAGIC Load the transformed data to persistent storage, so that it's query-able across notebooks and clusters

# COMMAND ----------

# MAGIC %md * Create a database and set it as default for this notebook

# COMMAND ----------

# MAGIC %sql 
# MAGIC   CREATE DATABASE IF NOT EXISTS STORMDB
# MAGIC   LOCATION "dbfs:/mnt/databricks-abhinav/noaa-storm/output"

# COMMAND ----------

# MAGIC %sql USE STORMDB

# COMMAND ----------

# MAGIC %md * Write/Overwrite the transformed data to a unmanaged table (data stored in S3)

# COMMAND ----------

transformedStormDf2.write.saveAsTable("STORM_EVENTS", format = "parquet", mode = "overwrite", partitionBy = "STATE_CODE", path = "dbfs:/mnt/databricks-abhinav/noaa-storm/output/tr-events")

# COMMAND ----------

# MAGIC %sql DESCRIBE STORM_EVENTS

# COMMAND ----------

# MAGIC %md ## Totally random stuff, ignore in context of notebook

# COMMAND ----------

# MAGIC %sql DROP TABLE default.STORM_EVENTS_DUP;

# COMMAND ----------

# MAGIC %sql SET spark.sql.files.maxRecordsPerFile=20000

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS default.STORM_EVENTS_DUP
# MAGIC USING PARQUET
# MAGIC --CLUSTERED BY (EVENT_ID) INTO 50 BUCKETS
# MAGIC SELECT * from STORMDB.STORM_EVENTS
# MAGIC CLUSTER BY EVENT_TYPE;

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val ldf = spark.table("STORMDB.STORM_EVENTS")
# MAGIC ldf.write.bucketBy(40, "EVENT_ID").sortBy("YEAR").format("parquet").mode("overwrite").saveAsTable("default.STORM_EVENTS_DUP")

# COMMAND ----------

# MAGIC %sql USE default

# COMMAND ----------

# MAGIC %sql CREATE DATASKIPPING INDEX ON STORM_EVENTS_DUP

# COMMAND ----------

# MAGIC %sql SELECT count(*) from default.STORM_EVENTS_DUP

# COMMAND ----------

tempdf = spark.table("default.STORM_EVENTS_DUP")
print(tempdf.rdd.getNumPartitions())

# COMMAND ----------

# MAGIC %md ## 4. Adhoc Analysis
# MAGIC Answer relevant questions

# COMMAND ----------

# MAGIC %md * What weather events have had most adverse impact on population health over the years?

# COMMAND ----------

# MAGIC %sql SELECT EVENT_TYPE, SUM(TOT_DEATHS) AS AGG_TOT_DEATHS, SUM(TOT_INJURIES) AS AGG_TOT_INJURIES
# MAGIC       FROM STORM_EVENTS
# MAGIC       GROUP BY EVENT_TYPE

# COMMAND ----------

# MAGIC %md * Which states have incurred maximum economic damage over the years?

# COMMAND ----------

# MAGIC %sql SELECT STATE_CODE, SUM(TOT_DAMAGE_THDS)
# MAGIC       FROM STORM_EVENTS
# MAGIC       WHERE STATE_CODE NOT IN ('GU','PR','VI','AS')
# MAGIC       GROUP BY STATE_CODE

# COMMAND ----------

# MAGIC %md * In Texas, what weather events have had maximum adverse impact each year, for last 10 years?

# COMMAND ----------

# MAGIC %sql
# MAGIC       SELECT YEAR, EVENT_TYPE, AGG_TOT_DMG_THDS
# MAGIC         FROM
# MAGIC         (
# MAGIC           SELECT EVENT_TYPE, YEAR, AGG_TOT_DMG_THDS,
# MAGIC                 RANK() OVER (PARTITION BY YEAR ORDER BY AGG_TOT_DMG_THDS DESC) dmg_rank
# MAGIC            FROM
# MAGIC             (
# MAGIC               SELECT EVENT_TYPE, YEAR, SUM(TOT_DAMAGE_THDS) AS AGG_TOT_DMG_THDS
# MAGIC               FROM STORM_EVENTS
# MAGIC               WHERE STATE_CODE = 'TX'
# MAGIC               AND YEAR > 2007
# MAGIC              GROUP BY EVENT_TYPE, YEAR
# MAGIC             ) tmp_in
# MAGIC           WHERE AGG_TOT_DMG_THDS IS NOT NULL AND AGG_TOT_DMG_THDS <> 0
# MAGIC         ) tmp_out
# MAGIC       WHERE dmg_rank <= 5
# MAGIC       ORDER BY YEAR

# COMMAND ----------

# MAGIC %md ## 5. Machine Learning
# MAGIC Create a simple machine learning model using GBT Classifier, which would determine type of fatality based on following features:
# MAGIC * Age of fatality
# MAGIC * Gender of fatality
# MAGIC * Fatality location

# COMMAND ----------

# MAGIC %md * Read the Storm Fatality dataset

# COMMAND ----------

fatalitiesDf = (spark.read.csv("dbfs:/mnt/databricks-abhinav/noaa-storm/input/fatalities", 
                         inferSchema=True, header=True, 
                         ignoreLeadingWhiteSpace=True, 
                         ignoreTrailingWhiteSpace=True))

# COMMAND ----------

# MAGIC %md * Join the transformed Storm Details dataset with Storm Fatalities dataset
# MAGIC 
# MAGIC (We're using the transformed dataframe directly, but one could also create a new dataframe from persistent table, particularly applicable in a different notebook)

# COMMAND ----------

eventFatalitiesDf = transformedStormDf2.join(fatalitiesDf.filter(fatalitiesDf['FATALITY_AGE'].isNotNull() & fatalitiesDf['FATALITY_SEX'].isNotNull()), ["EVENT_ID"])
display(eventFatalitiesDf)

# COMMAND ----------

# MAGIC %md 
# MAGIC * Create and test the model:
# MAGIC   * Index the fatality type lable
# MAGIC   * Bucketize the fatality age feature
# MAGIC   * Index other categorical features
# MAGIC   * Assemble all features into a Vector
# MAGIC   * Split above joined dataset into training and test
# MAGIC   * Create model by fitting a GBT Classifier-based pipeline over training data
# MAGIC   * Validate the model on test data

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorAssembler, QuantileDiscretizer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

labelIndexer = StringIndexer().setInputCol("FATALITY_TYPE").setOutputCol("label").fit(eventFatalitiesDf)

featureDiscretizer = QuantileDiscretizer(numBuckets=10, inputCol="FATALITY_AGE", outputCol="FAT_AGE_BIN").fit(eventFatalitiesDf)
featureIndexers = [StringIndexer().setInputCol(baseFeature).setOutputCol(baseFeature + "_IDX").fit(eventFatalitiesDf) for baseFeature in ["FATALITY_SEX", "FATALITY_LOCATION"]]

featureAssembler = VectorAssembler()
featureAssembler.setInputCols(["FATALITY_SEX_IDX", "FATALITY_LOCATION_IDX", "FAT_AGE_BIN"])
featureAssembler.setOutputCol("features")

labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
                               labels=labelIndexer.labels)

gbtClassifier = GBTClassifier(labelCol="label", featuresCol="features", maxIter=10)

pipelineStages = [labelIndexer, featureDiscretizer] + featureIndexers + [featureAssembler, gbtClassifier, labelConverter]
pipeline = Pipeline(stages=pipelineStages)

(trainingData, testData) = eventFatalitiesDf.randomSplit([0.7, 0.3])
model = pipeline.fit(trainingData)

predictions = model.transform(testData)
display(predictions.select("predictedLabel", "label", "features"))

# COMMAND ----------

# MAGIC %md * Find out the accuracy measure of the model
# MAGIC 
# MAGIC (We could get a much better model with more applicable features and more records for training)

# COMMAND ----------

evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g" % (1.0 - accuracy))