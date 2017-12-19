# Databricks notebook source
# MAGIC %md 
# MAGIC ![Architecture](https://s3-us-west-1.amazonaws.com/databricks-binu-mathew/image/deep_learning/jets.png)

# COMMAND ----------

# MAGIC %md 
# MAGIC ![Architecture](https://s3-us-west-1.amazonaws.com/databricks-binu-mathew/image/deep_learning/11.png)

# COMMAND ----------

from sparkdl import readImages
from pyspark.sql.functions import lit

img_dir = '/tmp/demo/planes'
passenger_jets_train_df = readImages(img_dir + "/passenger_jets").withColumn("label", lit(1))
fighter_jets_train_df = readImages(img_dir + "/fighter_jets").withColumn("label", lit(0))

#dataframe for training a classification model
train_df = passenger_jets_train_df.unionAll(fighter_jets_train_df)

#dataframe for testing the classification model
test_df = readImages('/bmathew/test_data')

# COMMAND ----------

import sys 
sys.stdout.isatty = lambda: False 
sys.stdout.encoding = 'utf-8' 

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from sparkdl import DeepImageFeaturizer 

featurizer = DeepImageFeaturizer(inputCol="image", outputCol="features", modelName="InceptionV3")
lr = LogisticRegression(maxIter=20, regParam=0.05, elasticNetParam=0.3, labelCol="label")
p = Pipeline(stages=[featurizer, lr])

p_model = p.fit(train_df)

# COMMAND ----------

from pyspark.sql.types import DoubleType
from pyspark.sql.functions import expr
def _p1(v):
  return float(v.array[1])
p1 = udf(_p1, DoubleType())

df = p_model.transform(test_df)
jets_df = df.select("image", (p1(df.probability)).alias("probability_passenger_jet"),(1-p1(df.probability)).alias("probability_fighter_jet"))

jets_df.registerTempTable('jets')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM jets 

# COMMAND ----------

