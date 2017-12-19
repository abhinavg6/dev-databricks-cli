# Databricks notebook source
# MAGIC %md ### Databricks ML model export library demo
# MAGIC 
# MAGIC This notebook demonstrates how to export a `LogisticRegressionModel` from MLlib to make predictions outside of Apache Spark, in any JVM-based application.
# MAGIC 
# MAGIC NOTE: Model export is currently supported in both Python and Scala, but models only be imported within JVM-based applications.
# MAGIC 
# MAGIC The basic workflow is as follows:
# MAGIC * In Databricks
# MAGIC   * Fit a model using MLlib.
# MAGIC   * Use `dbmlModelExport.ModelExport` to export the model as a set of JSON files.
# MAGIC * Move the model files to your deployment project or data store.
# MAGIC * In your JVM-based project (which does not need to depend upon Apache Spark)
# MAGIC   * Build your deployment project against the `dbml-local` library at https://dl.bintray.com/databricks/maven/com/databricks/dbml-local/
# MAGIC   * Within the application, load the saved model and make predictions.
# MAGIC 
# MAGIC This notebook demonstrates the part of the workflow in Databricks.  The accompanying demo application shows how to build a project against `dbml-local` and use the exported model.

# COMMAND ----------

# MAGIC %md ### Training and Pipeline Export

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StringIndexer, Tokenizer, HashingTF
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# COMMAND ----------

df = spark.read.parquet("/databricks-datasets/news20.binary/data-001/training").select("text", "topic")
df.cache()
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md #### Define ML Pipeline

# COMMAND ----------

labelIndexer = StringIndexer(inputCol="topic", outputCol="label", handleInvalid="keep")

# COMMAND ----------

tokenizer = Tokenizer(inputCol="text", outputCol="words")
hashingTF = HashingTF(inputCol="words", outputCol="features")

# COMMAND ----------

lr = LogisticRegression(maxIter=20)
pipeline = Pipeline(stages=[labelIndexer, tokenizer, hashingTF, lr])

# COMMAND ----------

# MAGIC %md #### Tune ML Pipeline

# COMMAND ----------

paramGrid = ParamGridBuilder().addGrid(hashingTF.numFeatures, [1000, 2000]).build()
cv = CrossValidator(estimator=pipeline, evaluator=MulticlassClassificationEvaluator(), estimatorParamMaps=paramGrid)

# COMMAND ----------

cvModel = cv.fit(df)

# COMMAND ----------

model = cvModel.bestModel

# COMMAND ----------

exampleResults = model.transform(df).select("topic", "label", "prediction", "text")
display(exampleResults)

# COMMAND ----------

# MAGIC %md #### Export Pipeline

# COMMAND ----------

from dbmlModelExport import ModelExport

# COMMAND ----------

for modelName in ModelExport.supportedModels:
    print(modelName)

# COMMAND ----------

# Can remove an old model file, if needed.
dbutils.fs.rm( "/tmp/ml_python_model_export/20news_pipeline", recurse=True)

# COMMAND ----------

ModelExport.exportModel(model, "/tmp/ml_python_model_export/20news_pipeline")

# COMMAND ----------

# MAGIC %fs ls dbfs:/tmp/ml_python_model_export/20news_pipeline

# COMMAND ----------

# MAGIC %md #### Download model files
# MAGIC 
# MAGIC Here, we use a hack to download the model files from the browser.  In general, you may want to programmatically move the model to S3 or another persistent storage layer.

# COMMAND ----------

# MAGIC %sh
# MAGIC zip -r /tmp/20news_pipeline.zip /dbfs/tmp/ml_python_model_export/20news_pipeline*
# MAGIC cp /tmp/20news_pipeline.zip /dbfs/FileStore/20news_pipeline.zip

# COMMAND ----------

# MAGIC %md  Get a link to the downloadable zip via:
# MAGIC `https://[MY_DATABRICKS_URL]/files/[FILE_NAME].zip`.  E.g., if you access Databricks at `https://mycompany.databricks.com`, then your link would be:
# MAGIC https://mycompany.databricks.com/files/20news_pipeline.zip

# COMMAND ----------

# MAGIC %md https://demo.cloud.databricks.com/files/20news_pipeline.zip