# Databricks notebook source
# MAGIC %sql USE EURO_SOCCER_DB

# COMMAND ----------

# MAGIC %sql 
# MAGIC     SELECT * FROM GAME_EVENTS 
# MAGIC     WHERE is_goal = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT CASE WHEN shot_place_str IS NULL THEN 'Unknown' ELSE shot_place_str END shot_place, COUNT(1) AS TOT_GOALS
# MAGIC     FROM GAME_EVENTS
# MAGIC     WHERE is_goal = 1
# MAGIC    GROUP BY shot_place_str

# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT country_code, COUNT(1) AS TOT_GOALS
# MAGIC     FROM GAME_EVENTS
# MAGIC     WHERE is_goal = 1
# MAGIC    GROUP BY country_code

# COMMAND ----------

# MAGIC %sql
# MAGIC       SELECT SHOT_PLACE_STR, LOCATION_STR, TOT_GOALS
# MAGIC         FROM
# MAGIC         (
# MAGIC           SELECT SHOT_PLACE_STR, LOCATION_STR, TOT_GOALS,
# MAGIC                 RANK() OVER (PARTITION BY SHOT_PLACE_STR ORDER BY TOT_GOALS DESC) goals_rank
# MAGIC            FROM
# MAGIC             (
# MAGIC               SELECT CASE WHEN LOCATION_STR IS NULL THEN 'Unknown' ELSE LOCATION_STR END LOCATION_STR, 
# MAGIC                     CASE WHEN SHOT_PLACE_STR IS NULL THEN 'Unknown' ELSE SHOT_PLACE_STR END SHOT_PLACE_STR, 
# MAGIC                     COUNT(1) AS TOT_GOALS
# MAGIC               FROM GAME_EVENTS
# MAGIC               WHERE is_goal = 1
# MAGIC               AND COUNTRY_CODE = 'ESP'
# MAGIC              GROUP BY SHOT_PLACE_STR, LOCATION_STR
# MAGIC             ) tmp_in
# MAGIC           WHERE TOT_GOALS IS NOT NULL AND TOT_GOALS <> 0
# MAGIC         ) tmp_out
# MAGIC       WHERE goals_rank <= 3
# MAGIC       AND LOCATION_STR != 'Unknown' 
# MAGIC       AND SHOT_PLACE_STR != 'Unknown'
# MAGIC       ORDER BY SHOT_PLACE_STR

# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT COUNTRY_CODE, TIME_BIN, COUNT(1) TOT_GOALS
# MAGIC    FROM GAME_EVENTS
# MAGIC    WHERE is_goal = 1
# MAGIC    GROUP BY COUNTRY_CODE, TIME_BIN
# MAGIC    ORDER BY COUNTRY_CODE, TIME_BIN

# COMMAND ----------

