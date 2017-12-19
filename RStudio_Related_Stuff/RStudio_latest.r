# Databricks notebook source
Sys.getenv("EXISTING_SPARKR_BACKEND_PORT")

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC dbutils.fs.put("/databricks/spark.R", """
# MAGIC options(max.print = 10000)
# MAGIC #options(repos = structure(c(CRAN = "http://cran.us.r-project.org")))
# MAGIC .libPaths(c(.libPaths(), "/home/ubuntu/databricks/spark/R/lib"))
# MAGIC initializeSparkR <- function() {
# MAGIC   backendPort <- as.integer(Sys.getenv("EXISTING_SPARKR_BACKEND_PORT"))
# MAGIC   if (is.na(backendPort)) {
# MAGIC     return("Please setup EXISTING_SPARKR_BACKEND_PORT environment variable.")
# MAGIC   }
# MAGIC   if (backendPort > 0) {
# MAGIC     library(SparkR)
# MAGIC     return(sparkR.init())
# MAGIC   } else {
# MAGIC     return("SparkR requires Spark 1.4+")
# MAGIC   }
# MAGIC }
# MAGIC initializeSqlContext <- function(sparkContext) {
# MAGIC   if (.sparkVersion >= "2.0") {
# MAGIC     return(sparkR.session())
# MAGIC   } else if (.sparkVersion >= "1.4") {
# MAGIC     ssc <- SparkR:::callJMethod(sparkContext, "sc")
# MAGIC     return(SparkR:::callJStatic("org.apache.spark.sql.SQLContext", "getOrCreate", ssc))
# MAGIC   } else {
# MAGIC     return("SparkR requires Spark 1.4+")
# MAGIC   }
# MAGIC }
# MAGIC getSparkVersion <- function(sparkContext) {
# MAGIC   if (is(sparkContext, "jobj")) {
# MAGIC     return(SparkR:::callJMethod(sparkContext, "version"))
# MAGIC   } else {
# MAGIC     return("1.3.0")
# MAGIC   }
# MAGIC }
# MAGIC .sc <- initializeSparkR()
# MAGIC .sparkVersion <- getSparkVersion(.sc)
# MAGIC sqlContext <- initializeSqlContext(.sc)
# MAGIC if (.sparkVersion >= "2.0") {
# MAGIC   spark <- sqlContext
# MAGIC }
# MAGIC """)

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.fs.rm("/databricks/spark.R")

# COMMAND ----------

library(SparkR)

# COMMAND ----------

# MAGIC %sh head /dbfs/databricks/spark.R

# COMMAND ----------

# MAGIC %md
# MAGIC Init Script for Installing RStudio on the server

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.fs.mkdirs("dbfs:/databricks/init/RStudio/")

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.fs.put("/databricks/init/RStudio/rstudio-install.sh","""
# MAGIC #!/bin/bash
# MAGIC sudo apt-get -y install gdebi-core
# MAGIC sudo wget https://download2.rstudio.org/rstudio-server-0.99.903-amd64.deb
# MAGIC sudo gdebi --n rstudio-server-0.99.903-amd64.deb
# MAGIC sudo adduser --disabled-password --gecos "" rstudio
# MAGIC sudo chpasswd <<<"rstudio:Rstudio123$"
# MAGIC """)

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.fs.rm("/databricks/init/RStudio/rstudio-install.sh")

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.fs.head("dbfs:/databricks/init/RStudio/rstudio-install.sh")

# COMMAND ----------

