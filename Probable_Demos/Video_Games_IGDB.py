# Databricks notebook source
dbutils.fs.rm("/mnt/databricks-abhinav/igdb-video-games", True)

# COMMAND ----------

dbutils.fs.mkdirs("/mnt/databricks-abhinav/igdb-video-games")
dbutils.fs.mkdirs("/mnt/databricks-abhinav/igdb-video-games/input")

# COMMAND ----------

import requests
import json
import sys

headers = {'user-key': 'ef05e3b05d7596fcad9b6981a67b0737', 'Accept': 'application/json'}

def get_data_from_igdb(genreId):
  genreRes = requests.get('https://api-2445582011268.apicast.io/genres/' + str(genreId), headers=headers)
  genreResJson = genreRes.json()

  for genre in genreResJson:
    genreAllGameIds = genre['games']
    genreGameIdChunks = [genreAllGameIds[i:i+100] for i in range(0, len(genreAllGameIds), 100)]
    for j, genreGameIdChunk in enumerate(genreGameIdChunks):
      fileName = genre['name'] + "_Chunk" + str(j) + ".json"
      fileName = fileName.replace(" ", "").replace("(", "").replace(")", "")
      print(fileName)

      gameIdsHttpPath = ','.join([str(gameId) for gameId in genreGameIdChunk])
      try:
        gamesRes = requests.get('https://api-2445582011268.apicast.io/games/' + gameIdsHttpPath, headers=headers)
        gamesResJson = gamesRes.json()

        filePath = '/dbfs/mnt/databricks-abhinav/igdb-video-games/input/' + fileName
        with open(filePath, 'w') as f:
          json.dump(gamesResJson, f)
      except:
        print("Error while getting game data for games " + gameIdsHttpPath + " - " + sys.exc_info()[0])
  
  print("Download completed for genre " + str(genreId))

# COMMAND ----------

[get_data_from_igdb(genre) for genre in [10,11,12,13,14,15,16,24]]

# COMMAND ----------

# MAGIC %fs ls /mnt/databricks-abhinav/igdb-video-games/input

# COMMAND ----------

# MAGIC %sh head /dbfs/mnt/databricks-abhinav/igdb-video-games/input/Racing_Chunk1.json

# COMMAND ----------

