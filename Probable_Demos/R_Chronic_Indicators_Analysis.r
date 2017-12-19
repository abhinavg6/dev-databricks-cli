# Databricks notebook source
# MAGIC %md
# MAGIC ![Databricks for Healthcare Analytics-1](http://cmcd.umichsites.org/wp-content/uploads/sites/59/2015/05/infographic-all1.png)
# MAGIC ![Databricks for Healthcare Analytics-2](https://npin.cdc.gov/sites/all/themes/custom/cdcnpin3/images/m-cdc-logo.png)
# MAGIC 
# MAGIC # Chronic Disease Indicators Analysis
# MAGIC * The chronic disease indicators (CDI) are a set of surveillance indicators developed by consensus among CDC, the Council of State and Territorial Epidemiologists (CSTE), and the National Association of Chronic Disease Directors (NACDD). CDI enables public health professionals and policymakers to retrieve uniformly defined state and selected metropolitan-level data for chronic diseases and risk factors that have a substantial impact on public health. These indicators are essential for surveillance, prioritization, and evaluation of public health interventions.
# MAGIC * In this report, we'll analyze the [CDI dataset](https://chronicdata.cdc.gov/Chronic-Disease-Indicators/U-S-Chronic-Disease-Indicators-CDI-/g4ie-h725), which covers 124 indicators in the following 18 topic groups: alcohol; arthritis; asthma; cancer; cardiovascular disease; chronic kidney disease; chronic obstructive pulmonary disease; diabetes; immunization; nutrition, physical activity, and weight status; oral health; tobacco; overarching conditions; and new topic areas that include disability, mental health, older adults, reproductive health, and school health.

# COMMAND ----------

library(SparkR)

# COMMAND ----------

# MAGIC %md ## 1. Data Engineering - ETL

# COMMAND ----------

# MAGIC %md
# MAGIC * Extract and explore raw data
# MAGIC * Transform and Load to a Databricks Table (Parquet on S3)

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/U_S__Chronic_Disease_Indicators-fda13.csv

# COMMAND ----------

# Read the dataset from DBFS
indicatorsDf <- read.df("/FileStore/tables/U_S__Chronic_Disease_Indicators-fda13.csv",
                    source = "csv", header="true", inferSchema = "true")

# Dimensions of the raw data
dim(indicatorsDf)

# COMMAND ----------

display(indicatorsDf)

# COMMAND ----------

count(filter(indicatorsDf, indicatorsDf$YearStart != indicatorsDf$YearEnd))

# COMMAND ----------

display(arrange(count(groupBy(indicatorsDf, indicatorsDf$Topic, indicatorsDf$DataValueType)), "Topic"))

# COMMAND ----------

display(arrange(distinct(indicatorsDf[,c("Stratification1","Stratification2","Stratification3")]), "Stratification1"))

# COMMAND ----------

# Filter relevant data for analysis
selectcols <- c("YearStart","LocationAbbr","LocationDesc","Topic","Question","DataValueType","DataValueUnit","Stratification1","DataValue")

indicatorsDfFiltered <- filter(indicatorsDf, (indicatorsDf$YearStart == indicatorsDf$YearEnd) & isNotNull(indicatorsDf$DataValue) & (trim(indicatorsDf$DataValue) != ''))
indicatorsDfFiltered <- select(indicatorsDfFiltered, selectcols)

indicatorsDfFiltered <- withColumnRenamed(indicatorsDfFiltered, "YearStart", "Year")
indicatorsDfFiltered <- withColumnRenamed(indicatorsDfFiltered, "LocationAbbr", "State_Code")
indicatorsDfFiltered <- withColumnRenamed(indicatorsDfFiltered, "LocationDesc", "State")
indicatorsDfFiltered <- withColumnRenamed(indicatorsDfFiltered, "Stratification1", "Stratification")

indicatorsDfFiltered <- arrange(indicatorsDfFiltered,"Year","State_Code","State","Topic","Question","DataValueType","DataValueUnit","Stratification")

dim(indicatorsDfFiltered)

# COMMAND ----------

# Transform the dataset before aggregation
transform <- function(x) {
  x <- {
    x$DataValueNum <- ifelse(x$DataValueType == "Yes/No", ifelse(x$DataValue == "Yes", 1, 0), as.numeric(x$DataValue))
    x$DataValueUnit <- ifelse(x$DataValueType == "Yes/No", "Binary", x$DataValueType)

    x[, c("Year","State_Code","State","Topic","Question","DataValueType","DataValueUnit","Stratification","DataValue","DataValueNum")]
  }
}

schema <- structType(structField("Year", "integer"), structField("State_Code", "string"),
                     structField("State", "string"), structField("Topic", "string"),
                     structField("Question", "string"), structField("DataValueType", "string"),
                     structField("DataValueUnit", "string"), structField("Stratification", "string"),
                     structField("DataValue", "string"), structField("DataValueNum", "double"))

indicatorsDfTransformed <- dapply(indicatorsDfFiltered, transform, schema)

# COMMAND ----------

display(indicatorsDfTransformed)

# COMMAND ----------

# Create a in-memory table for transformed data
registerTempTable(indicatorsDfTransformed, "Chronic_Disease_Indicators_Temp")

# COMMAND ----------

# MAGIC %sql 
# MAGIC   CREATE DATABASE IF NOT EXISTS CHRONIC_DISEASE_DB
# MAGIC   LOCATION "dbfs:/FileStore/databricks-abhinav/chronic-disease"

# COMMAND ----------

# MAGIC %sql USE CHRONIC_DISEASE_DB

# COMMAND ----------

# MAGIC %sql DROP TABLE CHRONIC_DISEASE_INDICATORS

# COMMAND ----------

# MAGIC %sql
# MAGIC   CREATE TABLE IF NOT EXISTS CHRONIC_DISEASE_INDICATORS
# MAGIC   USING PARQUET
# MAGIC   PARTITIONED BY (Year, Topic)
# MAGIC   LOCATION "dbfs:/FileStore/databricks-abhinav/chronic-disease/indicators"
# MAGIC   AS SELECT * FROM Chronic_Disease_Indicators_Temp

# COMMAND ----------

# MAGIC %md ## 2. Data Analysis

# COMMAND ----------

# MAGIC %sql DESCRIBE TABLE CHRONIC_DISEASE_INDICATORS

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC   select Stratification, COUNT(*) AS Sample_Count, SUM(DataValueNum) AS Tot_Cases
# MAGIC   from CHRONIC_DISEASE_INDICATORS
# MAGIC   where DataValueType = 'Number'
# MAGIC   and Topic = 'Diabetes'
# MAGIC   and Stratification not in ('Overall','Male','Female')
# MAGIC   group by Stratification

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC   select State_Code, SUM(DataValueNum) AS Tot_Cases
# MAGIC   from CHRONIC_DISEASE_INDICATORS
# MAGIC   where DataValueType = 'Number'
# MAGIC   and Topic = 'Diabetes'
# MAGIC   and Stratification not in ('Overall','Male','Female')
# MAGIC   group by State_Code
# MAGIC   order by Tot_Cases desc

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC   select State_Code, Year, SUM(DataValueNum) AS Tot_Cases
# MAGIC   from CHRONIC_DISEASE_INDICATORS
# MAGIC   where DataValueType = 'Number'
# MAGIC   and Topic = 'Diabetes'
# MAGIC   and Stratification not in ('Overall','Male','Female')
# MAGIC   and State_Code in ('FL','CA','NY','MI','NC','NJ')
# MAGIC   group by State_Code, Year
# MAGIC   order by State_Code, Year

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC   select State_Code, Stratification, Tot_Cases
# MAGIC   from
# MAGIC   (
# MAGIC       select State_Code, Stratification, Tot_Cases,
# MAGIC          RANK() over (partition by State_Code order by Tot_Cases desc) cases_rank
# MAGIC       from
# MAGIC       (
# MAGIC           select State_Code, Stratification, SUM(DataValueNum) AS Tot_Cases
# MAGIC           from CHRONIC_DISEASE_INDICATORS
# MAGIC           where DataValueType = 'Number'
# MAGIC           and Topic = 'Diabetes'
# MAGIC           and Stratification not in ('Overall','Male','Female')
# MAGIC           and State_Code in ('FL','CA','NY','MI','NC','NJ')
# MAGIC           group by State_Code, Stratification
# MAGIC       ) tmp_in
# MAGIC       where Tot_Cases IS NOT NULL AND Tot_Cases <> 0
# MAGIC   ) tmp_out
# MAGIC   where cases_rank <=3
# MAGIC   order by State_Code, Tot_Cases desc

# COMMAND ----------

# MAGIC %md ## 3. Data Science - Machine Learning

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Create a [Linear SVM](https://spark.apache.org/docs/latest/ml-classification-regression.html#linear-support-vector-machine) classification model for gender-based subset

# COMMAND ----------

indicatorsMLDf = sql("select State_Code, Topic, Question, DataValueType, DataValueUnit, Stratification, DataValueNum 
from CHRONIC_DISEASE_INDICATORS where DataValueNum is not null and Stratification in ('Male','Female')")
display(indicatorsMLDf)

# COMMAND ----------

trainingData <- sample(indicatorsMLDf, FALSE, 0.75)
testData <- except(indicatorsMLDf, trainingData)

print(paste0("Training data count: ", count(trainingData)))
print(paste0("Testing data count: ", count(testData)))

# COMMAND ----------

lsvmModel <- spark.svmLinear(trainingData,  Stratification ~ ., regParam = 0.01, maxIter = 10)
summary(lsvmModel)

# COMMAND ----------

predictions <- predict(lsvmModel, newData = testData)

# View predictions against price column
display(select(predictions, "Stratification", "prediction"))