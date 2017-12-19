// Databricks notebook source
// === Configurations for Kinesis streams ===
val awsAccessKeyId = get_Creds(awsAccessKeyId)
val awsSecretKey = get_Creds(awsSecretKey)
val kinesisRegion = "us-west-2" // e.g., "us-west-2"

// COMMAND ----------

val kinesisStreamName = "dbstream1"
val kinesisStreamName2 = "dbstream2"

// COMMAND ----------

import com.amazonaws.services.kinesis.model.PutRecordRequest
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.auth.{DefaultAWSCredentialsProviderChain, BasicAWSCredentials}
import java.nio.ByteBuffer
import scala.util.Random

// Verify that the Kinesis settings have been set
require(!awsAccessKeyId.contains("YOUR"), "AWS Access Key has not been set")
require(!awsSecretKey.contains("YOUR"), "AWS Access Secret Key has not been set")
require(!kinesisStreamName.contains("YOUR"), "Kinesis stream has not been set")
require(!kinesisRegion.contains("YOUR"), "Kinesis region has not been set")


// COMMAND ----------

val kinesis = spark.readStream
  .format("kinesis")
  .option("streamName", kinesisStreamName)
  .option("region", kinesisRegion)
  .option("initialPosition", "TRIM_HORIZON")
  .option("awsAccessKey", awsAccessKeyId)
  .option("awsSecretKey", awsSecretKey)
  .load()
val result = kinesis.selectExpr("lcase(CAST(data as STRING)) as word")
  .groupBy($"word")
  .count()
display(result)

// COMMAND ----------

// Create the low-level Kinesis Client from the AWS Java SDK.
import com.amazonaws.regions.Regions
import com.amazonaws.regions.Region
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder


val kinesisClient = new AmazonKinesisClient(new BasicAWSCredentials(awsAccessKeyId, awsSecretKey))


//kinesisClient.setRegion(Regions.fromName(kinesisRegion))

kinesisClient.setRegion(Region.getRegion(Regions.US_WEST_2))

var lastSequenceNumber: String = null

for (i <- 0 to 10) {
  val time = System.currentTimeMillis
  // Generate words: fox in sox
  for (word <- Seq("Sensor1", "Sensor2", "Sensor3", "Sensor4", "Sensor1", "Sensor3", "Sensor4", "Sensor5", "Sensor2", "Sensor3","Sensor1", "Sensor2","Sensor1", "Sensor2")) {
    val data = s"$word"
    val partitionKey = s"$word"
    val request = new PutRecordRequest()
        .withStreamName(kinesisStreamName)
        .withPartitionKey(partitionKey)
        .withData(ByteBuffer.wrap(data.getBytes()))
    if (lastSequenceNumber != null) {
      request.setSequenceNumberForOrdering(lastSequenceNumber)
    }    
    val result = kinesisClient.putRecord(request)
    lastSequenceNumber = result.getSequenceNumber()
  }
  Thread.sleep(math.max(5000 - (System.currentTimeMillis - time), 0)) // loop around every ~5 seconds 
}

// COMMAND ----------

val staticDf = spark.sql("SELECT * FROM product")

kinesis.join(staticDf, "partitionKey")          // inner equi-join with a static DF


// COMMAND ----------

val joinedStream = kinesis.join(staticDf, "partitionKey")

// COMMAND ----------

joinedStream.createOrReplaceTempView("joinedStream_tbl")

// COMMAND ----------

// MAGIC %sql select * from joinedStream_tbl

// COMMAND ----------

