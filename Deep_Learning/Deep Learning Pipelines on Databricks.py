# Databricks notebook source
# MAGIC %md # Introducing Deep Learning Pipelines for Apache Spark
# MAGIC 
# MAGIC Deep Learning Pipelines is a new library published by Databricks to provide high-level APIs for scalable deep learning model application and transfer learning via integration of popular deep learning libraries with MLlib Pipelines and Spark SQL. For an overview and the philosophy behind the library, check out the Databricks [blog post](https://databricks.com/blog/2017/06/06/databricks-vision-simplify-large-scale-deep-learning.html). This notebook parallels the [Deep Learning Pipelines README](https://github.com/databricks/spark-deep-learning), detailing usage examples with additional tips for getting started with the library on Databricks.

# COMMAND ----------

# MAGIC %md ## Cluster set-up
# MAGIC 
# MAGIC Deep Learning Pipelines is available as a Spark Package. To use it on your cluster, create a new library with the Source option "Maven Coordinate", using "Search Spark Packages and Maven Central" to find "spark-deep-learning". Then [attach the library to a cluster](https://docs.databricks.com/user-guide/libraries.html). To run this notebook, also create and attach the following libraries: 
# MAGIC * via PyPI: tensorflow, keras, h5py
# MAGIC * via Spark Packages: tensorframes
# MAGIC 
# MAGIC Deep Learning Pipelines is compatible with Spark versions 2.0 or higher and works with any instance type (CPU or GPU).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick User Guide
# MAGIC 
# MAGIC Deep Learning Pipelines provides a suite of tools around working with and processing images using deep learning. The tools can be categorized as
# MAGIC * **Working with images** natively in Spark DataFrames
# MAGIC * **Transfer learning**, a super quick way to leverage deep learning
# MAGIC * **Applying deep learning models at scale**, whether they are your own or known popular models, to image data to make predictions or transform them into features
# MAGIC * **Deploying models as SQL functions** to empower everyone by making deep learning available in SQL (coming soon)
# MAGIC * **Distributed hyper-parameter tuning** via Spark MLlib Pipelines (coming soon)
# MAGIC 
# MAGIC We'll cover each one with examples below.

# COMMAND ----------

# MAGIC %md Let us first get some images to work with in this notebook. We'll use the flowers dataset from the [TensorFlow retraining tutorial](https://www.tensorflow.org/tutorials/image_retraining).

# COMMAND ----------

# MAGIC %sh 
# MAGIC curl -O http://download.tensorflow.org/example_images/flower_photos.tgz
# MAGIC tar xzf flower_photos.tgz

# COMMAND ----------

display(dbutils.fs.ls('file:/databricks/driver/flower_photos'))

# COMMAND ----------

# The 'file:/...' directory will be cleared out upon cluster termination. That doesn't matter for this example notebook, but in most cases we'd want to store the images in a more permanent place. Let's move the files to dbfs so we can see how to work with it in the use cases below.
img_dir = '/tmp/flower_photos'
dbutils.fs.mkdirs(img_dir)
dbutils.fs.cp('file:/databricks/driver/flower_photos/tulips', img_dir + "/tulips", recurse=True)
dbutils.fs.cp('file:/databricks/driver/flower_photos/daisy', img_dir + "/daisy", recurse=True)
dbutils.fs.cp('file:/databricks/driver/flower_photos/LICENSE.txt', img_dir)
display(dbutils.fs.ls(img_dir))

# COMMAND ----------

# Let's create a small sample set of images for quick demonstrations.
sample_img_dir = img_dir + "/sample"
dbutils.fs.mkdirs(sample_img_dir)
files = dbutils.fs.ls(img_dir + "/tulips")[0:1] + dbutils.fs.ls(img_dir + "/daisy")[0:2]
for f in files:
  dbutils.fs.cp(f.path, sample_img_dir)
display(dbutils.fs.ls(sample_img_dir))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Working with images in Spark
# MAGIC 
# MAGIC The first step to applying deep learning on images is the ability to load the images. Deep Learning Pipelines includes utility functions that can load millions of images into a Spark DataFrame and decode them automatically in a distributed fashion, allowing manipulationg at scale.

# COMMAND ----------

from sparkdl import readImages
image_df = readImages(sample_img_dir)

# COMMAND ----------

# MAGIC %md The resulting DataFrame contains a string column named "filePath" containing the path to each image file, and a image struct ("`SpImage`") column called "image" containing the decoded image data.   

# COMMAND ----------

display(image_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transfer learning
# MAGIC Deep Learning Pipelines provides utilities to perform transfer learning on images, which is one of the fastest (code and run-time -wise) ways to start using deep learning. Using Deep Learning Pipelines, it can be done in just several lines of code.

# COMMAND ----------

# Create training & test DataFrames for transfer learning - this piece of code is longer than transfer learning itself below!
from sparkdl import readImages
from pyspark.sql.functions import lit

tulips_df = readImages(img_dir + "/tulips").withColumn("label", lit(1))
daisy_df = readImages(img_dir + "/daisy").withColumn("label", lit(0))
tulips_train, tulips_test = tulips_df.randomSplit([0.6, 0.4])
daisy_train, daisy_test = daisy_df.randomSplit([0.6, 0.4])
train_df = tulips_train.unionAll(daisy_train)
test_df = tulips_test.unionAll(daisy_test)
# Under the hood, each of the partitions is fully loaded in memory, which may be expensive.
# This ensure that each of the paritions has a small size.
train_df = train_df.repartition(100)
test_df = test_df.repartition(100)

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from sparkdl import DeepImageFeaturizer 

featurizer = DeepImageFeaturizer(inputCol="image", outputCol="features", modelName="InceptionV3")
lr = LogisticRegression(maxIter=20, regParam=0.05, elasticNetParam=0.3, labelCol="label")
p = Pipeline(stages=[featurizer, lr])

p_model = p.fit(train_df)

# COMMAND ----------

# MAGIC %md Note: the training step may take a while on Community Edition - try making a smaller training set in that case.

# COMMAND ----------

# MAGIC %md Let's see how well the model does:

# COMMAND ----------

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

tested_df = p_model.transform(test_df)
evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
print("Test set accuracy = " + str(evaluator.evaluate(tested_df.select("prediction", "label"))))

# COMMAND ----------

# MAGIC %md Not bad for a first try with zero tuning! Furthermore, we can look at where we are making mistakes:

# COMMAND ----------

from pyspark.sql.types import DoubleType
from pyspark.sql.functions import expr
def _p1(v):
  return float(v.array[1])
p1 = udf(_p1, DoubleType())

df = tested_df.withColumn("p_1", p1(tested_df.probability))
wrong_df = df.orderBy(expr("abs(p_1 - label)"), ascending=False)
display(wrong_df.select("filePath", "p_1", "label").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Applying deep learning models at scale
# MAGIC Spark DataFrames are a natural construct for applying deep learning models to a large-scale dataset. Deep Learning Pipelines provides a set of (Spark MLlib) Transformers for applying TensorFlow Graphs and TensorFlow-backed Keras Models at scale. In addition, popular images models can be applied out of the box, without requiring any TensorFlow or Keras code. The Transformers, backed by the Tensorframes library, efficiently handle the distribution of models and data to Spark workers.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Applying popular image models
# MAGIC There are many well-known deep learning models for images. If the task at hand is very similar to what the models provide (e.g. object recognition with ImageNet classes), or for pure exploration, one can use the Transformer `DeepImagePredictor` by simply specifying the model name.  

# COMMAND ----------

from sparkdl import readImages, DeepImagePredictor

image_df = readImages(sample_img_dir)

predictor = DeepImagePredictor(inputCol="image", outputCol="predicted_labels", modelName="InceptionV3", decodePredictions=True, topK=10)
predictions_df = predictor.transform(image_df)

display(predictions_df.select("filePath", "predicted_labels"))

# COMMAND ----------

# MAGIC %md Notice that the `predicted_labels` column shows "daisy" as a high probability class for all sample flowers using this base model. However, as can be seen from the differences in the probability values, the neural network has the information to discern the two flower types. Hence our transfer learning example above was able to properly learn the differences between daisies and tulips starting from the base model.

# COMMAND ----------

df = p_model.transform(image_df)
display(df.select("filePath", (1-p1(df.probability)).alias("p_daisy")))

# COMMAND ----------

# MAGIC %md
# MAGIC #### For TensorFlow users
# MAGIC Deep Learning Pipelines provides a MLlib Transformer that will apply the given TensorFlow Graph to a DataFrame containing a column of images (e.g. loaded using the utilities described in the previous section). Here is a very simple example of how a TensorFlow Graph can be used with the Transformer. In practice, the TensorFlow Graph will likely be restored from files before calling `TFImageTransformer`.

# COMMAND ----------

from sparkdl import readImages, TFImageTransformer
from sparkdl.transformers import utils
import tensorflow as tf

image_df = readImages(sample_img_dir)

g = tf.Graph()
with g.as_default():
    image_arr = utils.imageInputPlaceholder()
    resized_images = tf.image.resize_images(image_arr, (299, 299))
    # the following step is not necessary for this graph, but can be for graphs with variables, etc
    frozen_graph = utils.stripAndFreezeGraph(g.as_graph_def(add_shapes=True), tf.Session(graph=g), [resized_images])
      
transformer = TFImageTransformer(inputCol="image", outputCol="transformed_img", graph=frozen_graph,
                                 inputTensor=image_arr, outputTensor=resized_images,
                                 outputMode="image")
tf_trans_df = transformer.transform(image_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### For Keras users
# MAGIC For applying Keras models in a distributed manner using Spark, [`KerasImageFileTransformer`](link_here) works on TensorFlow-backed Keras models. It 
# MAGIC * Internally creates a DataFrame containing a column of images by applying the user-specified image loading and processing function to the input DataFrame containing a column of image URIs
# MAGIC * Loads a Keras model from the given model file path 
# MAGIC * Applies the model to the image DataFrame
# MAGIC 
# MAGIC The difference in the API from `TFImageTransformer` above stems from the fact that usual Keras workflows have very specific ways to load and resize images that are not part of the TensorFlow Graph.

# COMMAND ----------

# MAGIC %md To use the transformer, we first need to have a Keras model stored as a file. For this notebook we'll just save the Keras built-in InceptionV3 model instead of training one.

# COMMAND ----------

from keras.applications import InceptionV3

model = InceptionV3(weights="imagenet")
model.save('/tmp/model-full.h5')  # saves to the local filesystem
# move to a permanent place for future use
dbfs_model_path = 'dbfs:/models/model-full.h5'
dbutils.fs.cp('file:/tmp/model-full.h5', dbfs_model_path)  

# COMMAND ----------

# MAGIC %md Now on the prediction side:

# COMMAND ----------

from keras.applications.inception_v3 import preprocess_input
from keras.preprocessing.image import img_to_array, load_img
import numpy as np
from pyspark.sql.types import StringType
from sparkdl import KerasImageFileTransformer

def loadAndPreprocessKerasInceptionV3(uri):
  # this is a typical way to load and prep images in keras
  image = img_to_array(load_img(uri, target_size=(299, 299)))  # image dimensions for InceptionV3
  image = np.expand_dims(image, axis=0)
  return preprocess_input(image)

dbutils.fs.cp(dbfs_model_path, 'file:/tmp/model-full-tmp.h5')
transformer = KerasImageFileTransformer(inputCol="uri", outputCol="predictions",
                                        modelFile='/tmp/model-full-tmp.h5',  # local file path for model
                                        imageLoader=loadAndPreprocessKerasInceptionV3,
                                        outputMode="vector")

files = ["/dbfs" + str(f.path)[5:] for f in dbutils.fs.ls(sample_img_dir)]  # make "local" file paths for images
uri_df = sqlContext.createDataFrame(files, StringType()).toDF("uri")

keras_pred_df = transformer.transform(uri_df)

# COMMAND ----------

display(keras_pred_df.select("uri", "predictions"))

# COMMAND ----------

# MAGIC %md #### Clean up data generated for this notebook

# COMMAND ----------

dbutils.fs.rm(img_dir, recurse=True)
dbutils.fs.rm(dbfs_model_path)

# COMMAND ----------

# MAGIC %md ### Resources
# MAGIC * See the Databricks [blog post](https://databricks.com/blog/2017/06/06/databricks-vision-simplify-large-scale-deep-learning.html) announcing Deep Learning Pipelines for a high-level overview and more in-depth discussion of some of the concepts here.
# MAGIC * Check out the [Deep Learning Pipelines github page](https://github.com/databricks/spark-deep-learning).
# MAGIC * Learn more about [deep learning on Databricks](https://docs.databricks.com/applications/deep-learning/index.html).