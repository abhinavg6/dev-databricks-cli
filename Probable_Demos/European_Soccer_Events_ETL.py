# Databricks notebook source
# MAGIC %md
# MAGIC ![Europea Soccer Leagues](http://www.sofascore.com/news/wp-content/uploads/2016/04/Top5.jpg)
# MAGIC 
# MAGIC # European Soccer Events Analysis
# MAGIC * A soccer game generates many events and it is very important and interesting to take into account the context in which those events were generated.
# MAGIC * This dataset is a result of webscraping and integrating different data sources. All the events were derived by reverse engineering the text commentary, using regex.
# MAGIC * The dataset provides a granular view of 9,074 games, from the biggest 5 European football (soccer) leagues: England, Spain, Germany, Italy, France, from 2011/2012 season to 2016/2017 season as of 25.01.2017.

# COMMAND ----------

# MAGIC %md ##1. Data Sourcing/Extraction
# MAGIC 
# MAGIC Dataset has been downloaded from [**Kaggle**](https://www.kaggle.com/secareanualin/football-events). This is what it looks like:
# MAGIC 
# MAGIC * id_odsp - unique identifier of game (odsp stands from oddsportal.com)
# MAGIC * id_event - unique identifier of event (id_odsp + sort_order)
# MAGIC * sort_order - chronological sequence of events in a game
# MAGIC * time - minute of the game
# MAGIC * text - text commentary
# MAGIC * event_type - primary event, 11 unique events
# MAGIC * event_type2 - secondary event, 4 unique events
# MAGIC * side - Home or Away team
# MAGIC * event_team - team that produced the event. In case of Own goals, event team is the team that benefited from the own goal
# MAGIC * opponent - opposing team
# MAGIC * player - name of the player involved in main event
# MAGIC * player2 - name of player involved in secondary event
# MAGIC * player_in - player that came in (only applies to substitutions)
# MAGIC * player_out - player substituted (only applies to substitutions)
# MAGIC * shot_place - placement of the shot, 13 possible placement locations
# MAGIC * shot_outcome - 4 possible outcomes
# MAGIC * is_goal - binary variable if the shot resulted in a goal (own goals included)
# MAGIC * location - location on the pitch where the event happened, 19 possible locations
# MAGIC * bodypart - 3 body parts
# MAGIC * assist_method - in case of an assisted shot, 5 possible assist methods
# MAGIC * situation - 4 types

# COMMAND ----------

dbutils.fs.mkdirs("/mnt/databricks-abhinav/eu-soccer-events")
dbutils.fs.mkdirs("/mnt/databricks-abhinav/eu-soccer-events/input")
dbutils.fs.mkdirs("/mnt/databricks-abhinav/eu-soccer-events/interm")

# COMMAND ----------

# MAGIC %fs ls /mnt/databricks-abhinav/eu-soccer-events/input/

# COMMAND ----------

# MAGIC %sh head /dbfs/mnt/databricks-abhinav/eu-soccer-events/input/events.csv

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

eventsDf = (spark.read.csv("dbfs:/mnt/databricks-abhinav/eu-soccer-events/input/events.csv", 
                         schema=schema, header=True, 
                         ignoreLeadingWhiteSpace=True, 
                         ignoreTrailingWhiteSpace=True,
                         nullValue="NA"))
display(eventsDf)

# COMMAND ----------

gameInfDf = (spark.read.csv("dbfs:/mnt/databricks-abhinav/eu-soccer-events/input/ginf.csv", 
                         inferSchema=True, header=True, 
                         ignoreLeadingWhiteSpace=True, 
                         ignoreTrailingWhiteSpace=True,
                         nullValue="NA"))
display(gameInfDf)

# COMMAND ----------

# MAGIC %md ## 2. Data Transformation
# MAGIC Convert the data to a format, such that one could gather meaningful insights from it

# COMMAND ----------

def mapKeyToVal(mapping):
    def mapKeyToVal_(col):
        return mapping.get(col)
    return udf(mapKeyToVal_, StringType())

# COMMAND ----------

evtTypeMap = {0:'Announcement', 1:'Attempt', 2:'Corner', 3:'Foul', 4:'Yellow card', 5:'Second yellow card', 6:'Red card', 7:'Substitution', 8:'Free kick won', 9:'Offside', 10:'Hand ball', 11:'Penalty conceded'}

evtTyp2Map = {12:'Key Pass', 13:'Failed through ball', 14:'Sending off', 15:'Own goal'}

sideMap = {1:'Home', 2:'Away'}

shotPlaceMap = {1:'Bit too high', 2:'Blocked', 3:'Bottom left corner', 4:'Bottom right corner', 5:'Centre of the goal', 6:'High and wide', 7:'Hits the bar', 8:'Misses to the left', 9:'Misses to the right', 10:'Too high', 11:'Top centre of the goal', 12:'Top left corner', 13:'Top right corner'}

shotOutcomeMap = {1:'On target', 2:'Off target', 3:'Blocked', 4:'Hit the bar'}

locationMap = {1:'Attacking half', 2:'Defensive half', 3:'Centre of the box', 4:'Left wing', 5:'Right wing', 6:'Difficult angle and long range', 7:'Difficult angle on the left', 8:'Difficult angle on the right', 9:'Left side of the box', 10:'Left side of the six yard box', 11:'Right side of the box', 12:'Right side of the six yard box', 13:'Very close range', 14:'Penalty spot', 15:'Outside the box', 16:'Long range', 17:'More than 35 yards', 18:'More than 40 yards', 19:'Not recorded'}

bodyPartMap = {1:'Right foot', 2:'Left foot', 3:'Head'}

assistMethodMap = {0:'None', 1:'Pass', 2:'Cross', 3:'Headed pass', 4:'Through ball'}

situationMap = {1:'Open play', 2:'Set piece', 3:'Corner', 4:'Free kick'}

countryCodeMap = {'germany':'DEU', 'france':'FRA', 'england':'GBR', 'spain':'ESP', 'italy':'ITA'}

# COMMAND ----------

gameInfDf = gameInfDf.withColumn("country_code", mapKeyToVal(countryCodeMap)("country"))

display(gameInfDf['id_odsp','country','country_code'])

# COMMAND ----------

eventsDf = (
             eventsDf.
             withColumn("event_type_str", mapKeyToVal(evtTypeMap)("event_type")).
             withColumn("event_type2_str", mapKeyToVal(evtTyp2Map)("event_type2")).
             withColumn("side_str", mapKeyToVal(sideMap)("side")).
             withColumn("shot_place_str", mapKeyToVal(shotPlaceMap)("shot_place")).
             withColumn("shot_outcome_str", mapKeyToVal(shotOutcomeMap)("shot_outcome")).
             withColumn("location_str", mapKeyToVal(locationMap)("location")).
             withColumn("bodypart_str", mapKeyToVal(bodyPartMap)("bodypart")).
             withColumn("assist_method_str", mapKeyToVal(assistMethodMap)("assist_method")).
             withColumn("situation_str", mapKeyToVal(situationMap)("situation"))
           )

joinedDf = (
  eventsDf.join(gameInfDf, eventsDf.id_odsp == gameInfDf.id_odsp, 'inner').
  select(eventsDf.id_odsp, eventsDf.id_event, eventsDf.sort_order, eventsDf.time, eventsDf.event_type, eventsDf.event_type_str, eventsDf.event_type2, eventsDf.event_type2_str, eventsDf.side, eventsDf.side_str, eventsDf.event_team, eventsDf.opponent, eventsDf.player, eventsDf.player2, eventsDf.player_in, eventsDf.player_out, eventsDf.shot_place, eventsDf.shot_place_str, eventsDf.shot_outcome, eventsDf.shot_outcome_str, eventsDf.is_goal, eventsDf.location, eventsDf.location_str, eventsDf.bodypart, eventsDf.bodypart_str, eventsDf.assist_method, eventsDf.assist_method_str, eventsDf.situation, eventsDf.situation_str, gameInfDf.country_code)
)

# COMMAND ----------

from pyspark.ml.feature import QuantileDiscretizer

joinedDf = QuantileDiscretizer(numBuckets=10, inputCol="time", outputCol="time_bin").fit(joinedDf).transform(joinedDf)

display(joinedDf)

# COMMAND ----------

# MAGIC %md ## 3. Data Loading
# MAGIC Load the transformed data to persistent storage, so that it's query-able across notebooks and clusters

# COMMAND ----------

# MAGIC %sql 
# MAGIC   CREATE DATABASE IF NOT EXISTS EURO_SOCCER_DB
# MAGIC   LOCATION "dbfs:/mnt/databricks-abhinav/eu-soccer-events/interm"

# COMMAND ----------

# MAGIC %sql USE EURO_SOCCER_DB

# COMMAND ----------

joinedDf.write.saveAsTable("GAME_EVENTS", format = "parquet", mode = "overwrite", partitionBy = "COUNTRY_CODE", path = "dbfs:/mnt/databricks-abhinav/eu-soccer-events/interm/tr-events")

# COMMAND ----------

# MAGIC %sql DESCRIBE GAME_EVENTS

# COMMAND ----------

