// Databricks notebook source
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class GeometricMean(val defCount: Long = 0L, val defProduct: Double = 1.0) extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", DoubleType) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("count", LongType) ::
    StructField("product", DoubleType) :: Nil
  )

  // This is the output type of your aggregation function.
  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = defCount
    buffer(1) = defProduct
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Long](0) + 1
    buffer(1) = buffer.getAs[Double](1) * input.getAs[Double](0)
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
    buffer1(1) = buffer1.getAs[Double](1) * buffer2.getAs[Double](1)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    math.pow(buffer.getDouble(1), 1.toDouble / buffer.getLong(0))
  }
}

// COMMAND ----------

import org.apache.spark.sql.functions._

val ids = sqlContext.range(1, 20)
ids.createOrReplaceTempView("ids")
val df = sqlContext.sql("select id, id % 3 as group_id from ids")
df.createOrReplaceTempView("simple")

// COMMAND ----------

// Create an instance of UDAF GeometricMean.
val gm1 = new GeometricMean

// Show the geometric mean of values of column "id".
df.groupBy("group_id").agg(gm1(col("id")).as("GeometricMean")).show()

// COMMAND ----------

// Create an instance of UDAF GeometricMean.
val gm2 = new GeometricMean(defCount = 1L)

// Show the geometric mean of values of column "id".
df.groupBy("group_id").agg(gm2(col("id")).as("GeometricMean")).show()

// COMMAND ----------

// Create an instance of UDAF GeometricMean.
val gm3 = new GeometricMean(defCount = 1L, defProduct = 1.1)

// Show the geometric mean of values of column "id".
df.groupBy("group_id").agg(gm3(col("id")).as("GeometricMean")).show()

// COMMAND ----------

