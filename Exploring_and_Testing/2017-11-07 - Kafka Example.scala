// Databricks notebook source
// MAGIC %md ## Setup Connection to Kafka

// COMMAND ----------

import org.apache.spark.sql.functions.{get_json_object, json_tuple}
 
var streamingInputDF = 
  spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "<server:ip")
    .option("subscribe", "topic1")     
    .option("startingOffsets", "latest")  
    .option("minPartitions", "10")  
    .option("failOnDataLoss", "true")
    .load()

// COMMAND ----------

// MAGIC %md ## streamingInputDF.printSchema
// MAGIC 
// MAGIC   root <br><pre>
// MAGIC    </t>|-- key: binary (nullable = true) <br>
// MAGIC    </t>|-- value: binary (nullable = true) <br>
// MAGIC    </t>|-- topic: string (nullable = true) <br>
// MAGIC    </t>|-- partition: integer (nullable = true) <br>
// MAGIC    </t>|-- offset: long (nullable = true) <br>
// MAGIC    </t>|-- timestamp: timestamp (nullable = true) <br>
// MAGIC    </t>|-- timestampType: integer (nullable = true) <br>

// COMMAND ----------

// MAGIC %md ## Sample Message
// MAGIC <pre>
// MAGIC {
// MAGIC </t>"city": "<CITY>", 
// MAGIC </t>"country": "United States", 
// MAGIC </t>"countryCode": "US", 
// MAGIC </t>"isp": "<ISP>", 
// MAGIC </t>"lat": 0.00, "lon": 0.00, 
// MAGIC </t>"query": "<IP>", 
// MAGIC </t>"region": "CA", 
// MAGIC </t>"regionName": "California", 
// MAGIC </t>"status": "success", 
// MAGIC </t>"hittime": "2017-02-08T17:37:55-05:00", 
// MAGIC </t>"zip": "38917" 
// MAGIC }

// COMMAND ----------

// MAGIC %md ## GroupBy, Count

// COMMAND ----------

import org.apache.spark.sql.functions._

var streamingSelectDF = 
  streamingInputDF
   .select(get_json_object(($"value").cast("string"), "$.zip").alias("zip"))
    .groupBy($"zip") 
    .count()

// COMMAND ----------

// MAGIC %md ## Window

// COMMAND ----------

import org.apache.spark.sql.functions._

var streamingSelectDF = 
  streamingInputDF
   .select(get_json_object(($"value").cast("string"), "$.zip").alias("zip"), get_json_object(($"value").cast("string"), "$.hittime").alias("hittime"))
   .groupBy($"zip", window($"hittime".cast("timestamp"), "10 minute", "5 minute", "2 minute"))
   .count()


// COMMAND ----------

// MAGIC %md ## Memory Output

// COMMAND ----------

import org.apache.spark.sql.streaming.ProcessingTime

val query =
  streamingSelectDF
    .writeStream
    .format("memory")        
    .queryName("isphits")     
    .outputMode("complete") 
    .trigger(ProcessingTime("25 seconds"))
    .start()

// COMMAND ----------

// MAGIC %md ## Console Output

// COMMAND ----------

import org.apache.spark.sql.streaming.ProcessingTime

val query =
  streamingSelectDF
    .writeStream
    .format("console")        
    .outputMode("complete") 
    .trigger(ProcessingTime("25 seconds"))
    .start()

// COMMAND ----------

// MAGIC %md ## File Output with Partitions

// COMMAND ----------

import org.apache.spark.sql.functions._

var streamingSelectDF = 
  streamingInputDF
   .select(get_json_object(($"value").cast("string"), "$.zip").alias("zip"),    get_json_object(($"value").cast("string"), "$.hittime").alias("hittime"), date_format(get_json_object(($"value").cast("string"), "$.hittime"), "dd.MM.yyyy").alias("day"))
    .groupBy($"zip") 
    .count()
    .as[(String, String)]

// COMMAND ----------

import org.apache.spark.sql.streaming.ProcessingTime

val query =
  streamingSelectDF
    .writeStream
    .format("parquet")
    .option("path", "/mnt/sample/test-data")
    .option("checkpointLocation", "/mnt/sample/check")
    .partitionBy("zip", "day")
    .trigger(ProcessingTime("25 seconds"))
    .start()

// COMMAND ----------

// MAGIC %md ##### Create Table

// COMMAND ----------

// MAGIC %sql CREATE EXTERNAL TABLE  test_par
// MAGIC     (hittime string)
// MAGIC     PARTITIONED BY (zip string, day string)
// MAGIC     STORED AS PARQUET
// MAGIC     LOCATION '/mnt/sample/test-data'

// COMMAND ----------

// MAGIC %md ##### Add Partition

// COMMAND ----------

// MAGIC %sql ALTER TABLE test_par ADD IF NOT EXISTS
// MAGIC     PARTITION (zip='38907', day='08.02.2017') LOCATION '/mnt/sample/test-data/zip=38907/day=08.02.2017'

// COMMAND ----------

// MAGIC %md ##### Select

// COMMAND ----------

// MAGIC %sql select * from test_par

// COMMAND ----------

// MAGIC %md ## JDBC Sink

// COMMAND ----------

import java.sql._

class  JDBCSink(url:String, user:String, pwd:String) extends ForeachWriter[(String, String)] {
      val driver = "com.mysql.jdbc.Driver"
      var connection:Connection = _
      var statement:Statement = _
      
    def open(partitionId: Long,version: Long): Boolean = {
        Class.forName(driver)
        connection = DriverManager.getConnection(url, user, pwd)
        statement = connection.createStatement
        true
      }

      def process(value: (String, String)): Unit = {
        statement.executeUpdate("INSERT INTO zip_test " + 
                "VALUES (" + value._1 + "," + value._2 + ")")
      }

      def close(errorOrNull: Throwable): Unit = {
        connection.close
      }
   }


// COMMAND ----------

val url="jdbc:mysql://<mysqlserver>:3306/test"
val user ="user"
val pwd = "pwd"

val writer = new JDBCSink(url,user, pwd)
val query =
  streamingSelectDF
    .writeStream
    .foreach(writer)
    .outputMode("update")
    .trigger(ProcessingTime("25 seconds"))
    .start()

// COMMAND ----------

// MAGIC %md ## Kafka Sink

// COMMAND ----------

import java.util.Properties
import kafkashaded.org.apache.kafka.clients.producer._
import org.apache.spark.sql.ForeachWriter


 class  KafkaSink(topic:String, servers:String) extends ForeachWriter[(String, String)] {
      val kafkaProperties = new Properties()
      kafkaProperties.put("bootstrap.servers", servers)
      kafkaProperties.put("key.serializer", "kafkashaded.org.apache.kafka.common.serialization.StringSerializer")
      kafkaProperties.put("value.serializer", "kafkashaded.org.apache.kafka.common.serialization.StringSerializer")
      val results = new scala.collection.mutable.HashMap[String, String]
      var producer: KafkaProducer[String, String] = _

      def open(partitionId: Long,version: Long): Boolean = {
        producer = new KafkaProducer(kafkaProperties)
        true
      }

      def process(value: (String, String)): Unit = {
          producer.send(new ProducerRecord(topic, value._1 + ":" + value._2))
      }

      def close(errorOrNull: Throwable): Unit = {
        producer.close()
      }
   }

// COMMAND ----------

val topic = "<topic2>"
val brokers = "<server:ip>"

val writer = new KafkaSink(topic, brokers)

val query =
  streamingSelectDF
    .writeStream
    .foreach(writer)
    .outputMode("update")
    .trigger(ProcessingTime("25 seconds"))
    .start()