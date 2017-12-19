# Databricks notebook source
# MAGIC %md #NOAA Storm Data Analysis
# MAGIC 
# MAGIC Storms and other severe weather events can cause both public health and economic problems for communities and municipalities. Many severe events can result in fatalities, injuries, and property damage, and preventing such outcomes to the extent possible is a key concern. U.S. National Oceanic and Atmospheric Administration’s (NOAA) keeps a database, which tracks characteristics of major storms and weather events in the United States, including when and where they occur, as well as estimates of any fatalities, injuries, and property damage. In this report, we explore that database and answer what weather events have had most adverse impact on population health and economy over the years.
# MAGIC 
# MAGIC Data for this analysis comes from [here](https://d396qusza40orc.cloudfront.net/repdata%2Fdata%2FStormData.csv.bz2).

# COMMAND ----------

library(SparkR)

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/wmkt80i31505437379949/StormData.csv

# COMMAND ----------

# Read the dataset from DBFS
stormDf <- read.df("/FileStore/tables/wmkt80i31505437379949/StormData.csv",
                    source = "csv", header="true", inferSchema = "true")

# Dimensions of the raw data
dim(stormDf)

# COMMAND ----------

display(stormDf)

# COMMAND ----------

# Filter relevant data for analysis
permevents <- c("ASTRONOMICAL LOW TIDE","AVALANCHE","BLIZZARD","COASTAL FLOOD","COLD/WIND CHILL","DEBRIS FLOW","DENSE FOG","DENSE SMOKE","DROUGHT","DUST DEVIL","DUST STORM","EXCESSIVE HEAT","EXTREME COLD/WIND CHILL","FLASH FLOOD","FLOOD","FROST/FREEZE","FUNNEL CLOUD","FREEZING FOG","HAIL","HEAT","HEAVY RAIN","HEAVY SNOW","HIGH SURF","HIGH WIND","HURRICANE/TYPHOON","ICE STORM","LAKE-EFFECT SNOW","LAKESHORE FLOOD","LIGHTNING","MARINE HAIL","MARINE HIGH WIND","MARINE STRONG WIND","MARINE THUNDERSTORM WIND","RIP CURRENT","SEICHE","SLEET","STORM SURGE/TIDE","STRONG WIND","THUNDERSTORM WIND","TORNADO","TROPICAL DEPRESSION","TROPICAL STORM","TSUNAMI","VOLCANIC ASH","WATERSPOUT","WILDFIRE","WINTER STORM","WINTER WEATHER")

selectcols <- c("EVTYPE","STATE","COUNTY","BGN_DATE","FATALITIES","INJURIES","PROPDMG","PROPDMGEXP","CROPDMG","CROPDMGEXP")

stormDfFiltered <- filter(stormDf, (stormDf$EVTYPE %in% permevents) & (stormDf$PROPDMGEXP %in% c("K","M","B")) & (stormDf$CROPDMGEXP %in% c("K","M","B")))
stormDfFiltered <- select(stormDfFiltered, selectcols)
dim(stormDfFiltered)

# COMMAND ----------

display(stormDfFiltered)

# COMMAND ----------

# Transform the dataset before aggregation
transform <- function(x) {
  x <- {
    x$YEAR <- as.integer(as.character(as.Date(substr(x$BGN_DATE, 1, (regexpr("0:00:00", x$BGN_DATE) - 2)), format = "%m/%d/%Y"), format = "%Y"))

    x$POPIMPACT <- as.integer(x$FATALITIES) + as.integer(x$INJURIES)

    x$ECOIMPACT <- ifelse(x$PROPDMGEXP == "K", as.double(x$PROPDMG)/1000, ifelse(x$PROPDMGEXP == "B", as.double(x$PROPDMG)*1000, as.double(x$PROPDMG))) + ifelse(x$CROPDMGEXP == "K", as.double(x$CROPDMG)/1000, ifelse(x$CROPDMGEXP == "B", as.double(x$CROPDMG), as.double(x$CROPDMG)))
    x[, c("EVTYPE", "YEAR", "POPIMPACT", "ECOIMPACT")]
  }
}

schema <- structType(structField("EVTYPE", "string"), structField("YEAR", "integer"),
                     structField("POPIMPACT", "integer"), structField("ECOIMPACT", "double"))

stormDfTransformed <- dapply(stormDfFiltered, transform, schema)

# COMMAND ----------

# Create a in-memory table for transformed data
registerTempTable(stormDfTransformed, "Storm_Transformed")

# COMMAND ----------

# Aggregate the data by event type and collect at driver - If you use sparklyr, ddply could be used instead which will have better performance
stormAggByEvType = sql("select EVTYPE, sum(POPIMPACT) AS TOTPOPIMPACT, sum(ECOIMPACT) AS TOTECOIMPACT from Storm_Transformed group by EVTYPE")
localStormAggDf <- collect(stormAggByEvType)

# COMMAND ----------

# Look at the aggregate data using R visualization - ggplot2 could be used as well
par(mfrow = c(2,1), cex = 0.5)
with(head(localStormAggDf[order(localStormAggDf$TOTPOPIMPACT, decreasing = TRUE),], 10), barplot(TOTPOPIMPACT, names.arg=EVTYPE, col="LIGHTBLUE", xlab="Event Type", ylab="Total Population Impact", main="Population Impact of Events Across Years"))
with(head(localStormAggDf[order(localStormAggDf$TOTECOIMPACT, decreasing = TRUE),], 10), barplot(TOTECOIMPACT, names.arg=EVTYPE, col="MISTYROSE", xlab="Event Type", ylab="Total Economic Impact (in million dollars)", main="Economic Impact of Events Across Years"))

# COMMAND ----------

# Aggregate the data by event type, year and collect at driver
stormAggByEvTypeAndYr = sql("select EVTYPE, YEAR, sum(POPIMPACT) AS TOTPOPIMPACT, sum(ECOIMPACT) AS TOTECOIMPACT from Storm_Transformed group by EVTYPE, YEAR")
localStormAggDf2 <- collect(stormAggByEvTypeAndYr)

# COMMAND ----------

library(ggplot2)

# COMMAND ----------

ggplot(localStormAggDf2, aes(x=YEAR, y=TOTECOIMPACT)) + geom_line() + facet_wrap(~EVTYPE) + xlab("Year") + ylab("Total Economic Impact (in million dollars)")

# COMMAND ----------

