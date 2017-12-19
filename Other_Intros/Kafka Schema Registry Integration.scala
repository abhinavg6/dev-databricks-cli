// Databricks notebook source
// MAGIC %md #### You need to pull down the required libraries from the Confluent distribution
// MAGIC 
// MAGIC ##### The install package is available here: http://packages.confluent.io/archive/3.2/confluent-oss-3.2.2-2.11.tar.gz
// MAGIC ##### You need the following JARs:
// MAGIC 
// MAGIC ```
// MAGIC confluent-3.2.2/share/java/confluent-common/common-config-3.2.2.jar
// MAGIC confluent-3.2.2/share/java/confluent-common/common-utils-3.2.2.jar
// MAGIC confluent-3.2.2/share/java/schema-registry/kafka_2.11-0.10.2.1-cp2.jar
// MAGIC confluent-3.2.2/share/java/schema-registry/kafka-clients-0.10.2.1-cp2.jar
// MAGIC confluent-3.2.2/share/java/schema-registry/kafka-schema-registry-3.2.2.jar
// MAGIC confluent-3.2.2/share/java/schema-registry/kafka-schema-registry-client-3.2.2.jar
// MAGIC confluent-3.2.2/share/java/kafka-serde-tools/kafka-avro-serializer-3.2.2.jar
// MAGIC ```
// MAGIC 
// MAGIC #### To distribute JARs to your Databricks cluster, choose one of the 2 options below:

// COMMAND ----------

// requires an IAM role with read access to the specified bucket
// place the JARs above on an S3 bucket that your Databricks clusters can read
dbutils.fs.put("/databricks/init/schema-registry-jars.sh", """#!/bin/bash
pip install awscli
aws s3 cp s3://mybucket/schema-registry-jars/ /databricks/jars/ --recursive --include "*.jar"
""", true)

// COMMAND ----------

dbutils.fs.put("/databricks/init/schema-registry-jars.sh", """#!/bin/bash
wget http://packages.confluent.io/archive/3.2/confluent-oss-3.2.2-2.11.tar.gz
tar -xzf confluent-oss-3.2.2-2.11.tar.gz
cp confluent-3.2.2/share/java/confluent-common/common-config-3.2.2.jar /databricks/jars/
cp confluent-3.2.2/share/java/confluent-common/common-utils-3.2.2.jar /databricks/jars/
cp confluent-3.2.2/share/java/schema-registry/kafka_2.11-0.10.2.1-cp2.jar /databricks/jars/
cp confluent-3.2.2/share/java/schema-registry/kafka-clients-0.10.2.1-cp2.jar /databricks/jars/
cp confluent-3.2.2/share/java/schema-registry/kafka-schema-registry-3.2.2.jar /databricks/jars/
cp confluent-3.2.2/share/java/schema-registry/kafka-schema-registry-client-3.2.2.jar /databricks/jars/
cp confluent-3.2.2/share/java/kafka-serde-tools/kafka-avro-serializer-3.2.2.jar /databricks/jars/
""", true)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Confirm you have access to the schema registry by running the following command in a notebook (change `localhost` to your actual schema registry host)
// MAGIC 
// MAGIC ```
// MAGIC %sh curl http://localhost:8081
// MAGIC ```
// MAGIC 
// MAGIC ### Once you have the JARs on the cluster, you can define your decoder as follows (assumes Scala 2.11 cluster)
// MAGIC 
// MAGIC #### Be aware you need to replace `localhost` with your specific schema registry URL
// MAGIC 
// MAGIC ```
// MAGIC import org.apache.spark.sql.functions._
// MAGIC import org.apache.spark.sql.types._
// MAGIC import org.apache.avro.generic.GenericRecord
// MAGIC 
// MAGIC object MySchemaRegistryDecoder {
// MAGIC   val client = new io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient("http://localhost:8081", 10)
// MAGIC   val valueDecoder = new io.confluent.kafka.serializers.KafkaAvroDecoder(client)
// MAGIC   
// MAGIC   def decode(message: Array[Byte]) = {
// MAGIC     val record = valueDecoder.fromBytes(message).asInstanceOf[GenericRecord]
// MAGIC     
// MAGIC     /**
// MAGIC     Expected schema for this record is
// MAGIC     
// MAGIC     {
// MAGIC       "fields": [
// MAGIC           { "name": "str1", "type": "string" },
// MAGIC           { "name": "int1", "type": "int" }
// MAGIC       ],
// MAGIC       "name": "myrecord",
// MAGIC       "type": "record"
// MAGIC     }
// MAGIC     
// MAGIC     **/
// MAGIC     
// MAGIC     (record.get("str1").toString, record.get("int1").asInstanceOf[Int])
// MAGIC   }
// MAGIC }
// MAGIC 
// MAGIC val recordType = new StructType()
// MAGIC   .add("str1", StringType)
// MAGIC   .add("int1", IntegerType)
// MAGIC 
// MAGIC val decodeRecord = udf(MySchemaRegistryDecoder.decode _, recordType)
// MAGIC ```
// MAGIC 
// MAGIC ### ...and then use it as a UDF like so:
// MAGIC 
// MAGIC ```
// MAGIC display(df.select(decodeRecord($"value").as("record")).select($"record.*"))
// MAGIC ```