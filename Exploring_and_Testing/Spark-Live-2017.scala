// Databricks notebook source
// MAGIC %md ##Spark Live with Databricks and Apache Spark 2.2

// COMMAND ----------

// MAGIC %md #### Class Logistics and Operations
// MAGIC * Start, End
// MAGIC * Questions
// MAGIC * Lunch
// MAGIC * Breaks

// COMMAND ----------

// MAGIC %md #### Topics
// MAGIC 
// MAGIC   * Background / Architecture
// MAGIC   * Querying Data with DataFrame, Dataset, SQL
// MAGIC   * Brief Intro to ...
// MAGIC     * RDDs
// MAGIC     * Structured Streaming
// MAGIC     * SparkML Machine Learning

// COMMAND ----------

// MAGIC %fs ls /databricks-datasets/amazon/

// COMMAND ----------

// MAGIC %sql SELECT * FROM parquet.`/databricks-datasets/amazon/data20K`

// COMMAND ----------

// MAGIC %sql SELECT count(1), rating FROM parquet.`/databricks-datasets/amazon/data20K` GROUP BY rating

// COMMAND ----------

// MAGIC %sql SELECT * FROM parquet.`/databricks-datasets/amazon/data20K` WHERE rating = 1 AND review LIKE '%awesome%'
// MAGIC 
// MAGIC -- What kind of SQL is this? Spark 2.0 supports the SQL:2003 standard as well as HiveQL.

// COMMAND ----------

// MAGIC %md ####Introduction
// MAGIC * What is Spark?
// MAGIC   * Spark is a *distributed*, *data-parallel* compute engine ... with lots of other goodies around to make the work easier for common tasks
// MAGIC * How is Spark different from ... 
// MAGIC   * Hadoop?
// MAGIC   * RDBMSs?

// COMMAND ----------

// MAGIC %md ####Basic Architecture
// MAGIC * Spark cluster / Spark application
// MAGIC   * Driver
// MAGIC   * Executors
// MAGIC   
// MAGIC <img src="http://i.imgur.com/h621Rva.png" width="700px"></img>

// COMMAND ----------

// MAGIC %md #### Spark application(s) vs. Underlying cluster
// MAGIC 
// MAGIC * Spark *applications*, with their drivers and executors, are sometimes referred to as "Spark Clusters"
// MAGIC   * __But__ that "Spark Cluster" is not normally a long-running thing
// MAGIC   * One does not normally "deploy machines (or VMs, or Containers) for a Spark cluster" in the absence of a specific Spark application
// MAGIC   
// MAGIC * The underlying cluster -- most commonly YARN -- may have long-running nodes (hardware, VMs, etc.)
// MAGIC   * *Multiple* Apache Spark application ("Spark clusters") can be launched in that cluster
// MAGIC   * This pattern allows multiple users, teams, departments, etc. to run independent Spark applications on a common, shared infrastructure
// MAGIC 
// MAGIC 
// MAGIC <img src="http://i.imgur.com/vJ55hxW.png" width="800px"></img>

// COMMAND ----------

// MAGIC %md ####Programming Spark
// MAGIC * Several ways
// MAGIC   * Interactive (shell, notebooks)
// MAGIC   * Scripts
// MAGIC   * Compiled Programs
// MAGIC * Languages
// MAGIC   * SQL
// MAGIC   * Scala
// MAGIC   * Python
// MAGIC   * R
// MAGIC   * Java
// MAGIC   * C#, F# (via Mobius)
// MAGIC   * JavaScript (via Eclair)
// MAGIC   * others
// MAGIC   
// MAGIC Principally: Scala, SQL, Python, R

// COMMAND ----------

// MAGIC %md ####Let's Get Some Data!
// MAGIC 
// MAGIC The Wikimedia Project hosts a ton of analytics data -- in addition to their regular content. 
// MAGIC 
// MAGIC Let's take a look at what pages were most popular on the day of Donald Trump's inauguration...

// COMMAND ----------

val ACCESS_KEY_ID = "AKIAJBRYNXGHORDHZB4A"
val SECRET_ACCESS_KEY = "a0BzE1bSegfydr3%2FGE3LSPM6uIV5A4hOUfpH8aFF" 
val BUCKET = "spark-live"
val MOUNT = "/mnt/spark-live"

try {
  dbutils.fs.mount("s3a://"+ ACCESS_KEY_ID + ":" + SECRET_ACCESS_KEY + "@" + BUCKET, MOUNT)
} catch {
  // may be already mounted...
  case t:Throwable => println(t)
}

// COMMAND ----------

// MAGIC %sh ls /dbfs/mnt/spark-live

// COMMAND ----------

// MAGIC %md How do we just take a look into a file?

// COMMAND ----------

spark.read.text("/mnt/spark-live/pageviews-20170120-180000.gz").show(false)

// COMMAND ----------

spark.read.option("delimiter", " ").csv("/mnt/spark-live/pageviews-20170120-180000.gz").show

// COMMAND ----------

// MAGIC %md Let's fix the schema. We know (from Wikimedia) that the first three columns are:
// MAGIC   * `project : string`
// MAGIC   * `page : string`
// MAGIC   * `requests : int`	
// MAGIC   * we can ignore the 4th column -- we won't be using it
// MAGIC   
// MAGIC One way to do this is to use `selectExpr` and SQL snippets to `CAST` and rename the columns:

// COMMAND ----------

val wikimediaData = spark.read
                          .option("delimiter", " ")
                          .csv("/mnt/spark-live/pageviews-20170120-180000.gz")
                          .selectExpr("_c0 AS project", "_c1 AS page", "CAST(_c2 AS INT) AS requests")

// COMMAND ----------

// MAGIC %md Let's give this table (really a view, or query) a name ... so that we can look at it with SQL:

// COMMAND ----------

wikimediaData.createOrReplaceTempView("pageviews")

// COMMAND ----------

// MAGIC %sql SHOW TABLES;

// COMMAND ----------

// MAGIC %sql DESCRIBE pageviews;

// COMMAND ----------

// MAGIC %sql SELECT count(*) FROM pageviews WHERE project = 'en';

// COMMAND ----------

// MAGIC %sql SELECT * FROM pageviews WHERE project = 'en' ORDER BY requests DESC;

// COMMAND ----------

// MAGIC %sql SELECT * FROM pageviews WHERE project = 'en' AND page LIKE '%Spark%' ORDER BY requests DESC;

// COMMAND ----------

// MAGIC %md We can write all kinds of SQL queries (SQL:2003 + HiveQL ... documented at https://docs.databricks.com/spark/latest/spark-sql/index.html#spark-sql-language-manual) 
// MAGIC 
// MAGIC ... and we can use a programmatic API or "domain-specific language" as well.
// MAGIC 
// MAGIC The DataFrame/Dataset API allows us to write native Python, Java, Scala, or R programs using Spark.
// MAGIC 
// MAGIC Common tasks are fairly similar across these APIs:
// MAGIC 
// MAGIC |SQL|DataFame API|DataFrame example (with String column names)|
// MAGIC |---|---|---|
// MAGIC |SELECT|select, selectExpr|myDataFrame.select("someColumn")|
// MAGIC |WHERE|filter, where|myDataFrame.filter("someColumn > 10")|
// MAGIC |GROUP BY|groupBy|myDataFrame.groupBy("someColumn")|
// MAGIC |ORDER BY|orderBy|myDataFrame.orderBy("column")|
// MAGIC |JOIN|join|myDataFrame.join(otherDataFrame, "innerEquiJoinColumn")|
// MAGIC |UNION|union|myDataFrame.union(otherDataFrame)|
// MAGIC 
// MAGIC The API support -- both for SQL and DataFrame/Dataset -- is far broader than these examples. E.g., columns and expressions can be written in a strongly-typed manner, using Column objects. These can be generated from Strings or Symbols; many types of JOIN are supported; and a variety of mechanisms for creating a DataFrame from a source exist in all of these APIs.

// COMMAND ----------

// Here's the earlier query with the DataFrame/Dataset API:

val query = spark.table("pageviews").filter("project = 'en'").filter('page like "%Spark%").orderBy('requests desc)

display(query)

// COMMAND ----------

// Let's take that apart:

val query = spark.table("pageviews")

// COMMAND ----------

display(query)

// COMMAND ----------

val step1 = spark.table("pageviews")

val step2 = step1.filter("project = 'en'")

// COMMAND ----------

val step1 = spark.table("pageviews")

val step2 = step1.filter("project = 'en'")

val step3 = step2.filter('page like "%Spark%") // what is 'page here? A Scala symbol that is converted to a column object in context

// COMMAND ----------

display(step3)

// COMMAND ----------

//Show me this "page" column object

val theDataFrame = spark.table("pageviews")

val theColumn = theDataFrame("page")

// COMMAND ----------

// So what is 'page like "%Spark%"? An API call on Column, that returns another Column:

theColumn.like("%Spark%") // Java-style syntax, same call

// COMMAND ----------

// MAGIC %md ####Where do these API docs live?
// MAGIC 
// MAGIC 1. Dataset class (DataFrame is a limited but very useful form of Dataset ... it is a Dataset[Row])
// MAGIC 2. Column
// MAGIC 3. RelationalGroupedDataset and KeyValueGroupedDataset
// MAGIC 4. *tons* of helpful functions in org.apache.spark.sql.functions (package object) http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$

// COMMAND ----------

//One more:

display(spark.table("pageviews").groupBy("project").count().orderBy('count desc))

// COMMAND ----------

// MAGIC %md #### Hopefully that seemed reasonable ... 
// MAGIC 
// MAGIC __but__ ... it's taking too long!
// MAGIC * ... turns out our Spark code is fine in terms of API ... but the execution underneath isn't quite right yet.
// MAGIC 
// MAGIC How is that possible? We've barely even done anything yet and something is wrong?
// MAGIC * How would we even know? How can we fix it? 
// MAGIC 
// MAGIC Let's figure it out:

// COMMAND ----------

// MAGIC %md ####Spark is a parallel cluster computing tool...
// MAGIC 
// MAGIC so we're using lots of nodes or at least threads, right?
// MAGIC 
// MAGIC Take a look at the parallelism in that last query. In fact, we're only using 1 thread.
// MAGIC 
// MAGIC We'll talk about why, and about how to fix it, but first, let's get some more terminology on the table
// MAGIC * Application
// MAGIC * Query
// MAGIC * Job
// MAGIC * Stage
// MAGIC * Task
// MAGIC 
// MAGIC Now we can at least use the Spark Graphical UI to see that we did all of that work with just one thread!

// COMMAND ----------

// MAGIC %md What was the problem? 
// MAGIC 
// MAGIC Recall the data came from wikimedia as a `gzip` file, and I didn't do any "magic" preparing it for today: I just dropped that .gz into the S3 bucket that we pointed Spark at.
// MAGIC 
// MAGIC Unfortunately, gzip is not a *splittable* compression format. So Spark can't read different pieces of it in parallel using different tasks.
// MAGIC 
// MAGIC Instead we'll convert to plain old uncompressed CSV:

// COMMAND ----------

dbutils.fs.rm("/FileStore/pageviews-csv", true) // delete any data from the destination folder (in case we run this notebook multiple times)

// COMMAND ----------

spark.table("pageviews").write.option("header", true).csv("/FileStore/pageviews-csv")

// COMMAND ----------

// MAGIC %md That takes a while ... remember we have to process it in one thread

// COMMAND ----------

spark.table("pageviews").count // USING 1 TASK

// COMMAND ----------

spark.read.csv("/FileStore/pageviews-csv").count // PARALLEL?

// COMMAND ----------

// MAGIC %md That's noticeably faster, and uses 8 parallel tasks. (Remember we only have ~1 real CPU core here, so we wouldn't expect a 8x speedup)
// MAGIC 
// MAGIC Notice:
// MAGIC * 2 Jobs
// MAGIC * Reading is done in parallel 
// MAGIC   * How is the parallelism determined?
// MAGIC     * With SparkSQL in 2.0 see https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/DataSourceScanExec.scala
// MAGIC     * Main number comes from defaultParallelism which is usually the total number of cores available    
// MAGIC * Aggregation is required

// COMMAND ----------

// Let's add in a filter step and see what happens ...

spark.read.option("header", true).csv("/FileStore/pageviews-csv").filter('project==="en").count

// COMMAND ----------

// MAGIC %md Notice that adding a filter took a little longer -- since more computation was happening -- but it did not add any tasks (or stages, jobs, etc.). 
// MAGIC 
// MAGIC This -- inlining of "map" operations into tasks -- is called pipelining and is a key part of the Spark architecture. Tasks read from storage (or an external system, previous shuffle, etc.), compose *all* of the narrow operations on a partition, and then write the data out to storage, shuffle, or the driver. 
// MAGIC 
// MAGIC *So intermediate results never need to be materialized!* (this is key to handling large volumes of data efficienty)

// COMMAND ----------

// MAGIC %md Now we've changed formats, and we're reading a file source (no table name) and using Scala ... but is the CSV version of the data the one we really want?
// MAGIC 
// MAGIC CSV is not the most performant (or compact) format we can use.
// MAGIC 
// MAGIC Other things being equal, parquet is a the default recommended file format. It is a columnar, compressed file format based on the Google's Dremel paper, and it generally provides great performance on Spark, Impala, Presto and other data tools. It even supports partitioning based on the contents, so that queries don't need to process the entire dataset to find the data they are looking for.
// MAGIC 
// MAGIC <img src="https://i.imgur.com/ex7zY3U.png" width=500/>
// MAGIC 
// MAGIC Let's rewrite our data to parquet:

// COMMAND ----------

dbutils.fs.rm("/FileStore/pageviews", true) // clear out the destination folder in case we run this notebook multiple times

// COMMAND ----------

spark.table("pageviews").write.parquet("/FileStore/pageviews")

// COMMAND ----------

// MAGIC %md ... and give the new dataset a table name:

// COMMAND ----------

spark.read.parquet("/FileStore/pageviews").filter('project==="en").createOrReplaceTempView("pv")

// COMMAND ----------

// MAGIC %md Now we can try out all sort of interesting SQL queries...

// COMMAND ----------

// MAGIC %sql SELECT * FROM pv WHERE page = 'New_York'

// COMMAND ----------

// MAGIC %md *Advanced Topic:* How would we speed this sort of query up in a RDBMS, or with Hive? Probably with an index.
// MAGIC 
// MAGIC Spark does not support indices, so this flavor of query -- matching a very small fraction of records -- is not where Spark's performance shines in the default configuration.
// MAGIC 
// MAGIC What do you do if you need this sort of functionality? There are a number of options, depending on your problem and architecture, but remember: 
// MAGIC 
// MAGIC Spark is a whole ecosystem, and you're not limited to just code that ships in Apache or from Databricks. E.g., one thing to look at is Intel's __Optimized Analytics Package for Spark Platform__ which adds index support: https://github.com/Intel-bigdata/OAP

// COMMAND ----------

// MAGIC %md Aside from noticing that your app doesn't perform well, what is another way to understand how Spark plans to run your query?
// MAGIC 
// MAGIC We can ask Spark to *EXPLAIN* in SQL or via the API.
// MAGIC 
// MAGIC Here is an example query -- first, showing results; and then explaining the optimizations applied by the Catalyst rule-based optimizer:

// COMMAND ----------

spark.read.parquet("/FileStore/pageviews").filter('project==="en").select('page).filter('page like "Apache%").show

// COMMAND ----------

spark.read.parquet("/FileStore/pageviews").filter('project==="en").select('page).filter('page like "Apache%").explain(true)

// COMMAND ----------

// MAGIC %md *Advanced topic*: Dive deep into supported qualitative optimizations by looking at https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala

// COMMAND ----------

// MAGIC %md What about data that we *do* know about and query a lot ... use cases borrowed from the world of analytical databases? 
// MAGIC 
// MAGIC __Spark 2.2: "Cost-based Optimizer Framework"__ picks up where Catalyst's existing optimizations leave off. You can see which features are included in the initial release, along with links to design, discussion, and code at https://issues.apache.org/jira/browse/SPARK-16026
// MAGIC 
// MAGIC As an Apache open-source project, Spark's JIRAs are public, searchable, and provide great insight into the roadmap and evolution of Spark.
// MAGIC 
// MAGIC Let's take a quick look at the cost-based optimizer ("CBO") in action!

// COMMAND ----------

// MAGIC %md __Join reordering__
// MAGIC 
// MAGIC In previous versions of Spark, joins were typically ordered based on their appearance in the query, without regard for the size of the data. This sometimes lead to inefficient naïve join ordering:

// COMMAND ----------

spark.range(10000000).write.saveAsTable("biglist")
spark.range(10000000).withColumn("square", 'id * 'id).write.saveAsTable("listsquares")
spark.range(1000).write.saveAsTable("small")

// COMMAND ----------

spark.table("biglist").join(spark.table("listsquares"), "id").join(spark.table("small"), "id").collect

// COMMAND ----------

// MAGIC %md Take a look at the Spark UI ... Look at the SQL tab, then the description link for the most recent query to see the executed plan.
// MAGIC 
// MAGIC Pretty disappointing! Spark did an expensive Sort-Merge join on all 10,000,000 rows ... and only later completed the small inner join to yield 1,000 rows.
// MAGIC 
// MAGIC Let's turn on Spark 2.2 CBO and join reordering:

// COMMAND ----------

// MAGIC %sql SET spark.sql.cbo.enabled = true

// COMMAND ----------

// MAGIC %sql SET spark.sql.cbo.joinReorder.enabled = true

// COMMAND ----------

// MAGIC %md For backwards compatibility, new features are typically "opt-in" for a release or two; the CBO will be default in the future.
// MAGIC 
// MAGIC Next, let's compute table-level stats for our tables:

// COMMAND ----------

// MAGIC %sql ANALYZE TABLE biglist COMPUTE STATISTICS;
// MAGIC ANALYZE TABLE listsquares COMPUTE STATISTICS;
// MAGIC ANALYZE TABLE small COMPUTE STATISTICS

// COMMAND ----------

spark.table("biglist").join(spark.table("listsquares"), "id").join(spark.table("small"), "id").collect

// COMMAND ----------

// MAGIC %md Look at the executed plan for this version ... the join is reordered to avoid the large (10,000,000 row <-> 10,000,000 row) join.
// MAGIC 
// MAGIC Now let's look at one more feature. What if we add a filter condition so that we only keep 100 rows from the `squares` table. In that case, the "original" plan might be better -- join the big table to the tiny list of squares, yielding 100 rows, then join the `small` table.
// MAGIC 
// MAGIC As a baseline, let's observe that Spark doesn't do this optimization, because it doesn't know the distribution of data in the `square` column, so it can't yet estimate how many rows are involved:

// COMMAND ----------

spark.table("biglist").join(spark.table("listsquares").filter('square < 10000), "id").join(spark.table("small"), "id").collect

// COMMAND ----------

// MAGIC %md Note the SQL UI shows the same query plan as before -- adding the `filter` didn't change anything for Spark.
// MAGIC 
// MAGIC Let's have Spark build column-level statistics on that table and try again:

// COMMAND ----------

// MAGIC %sql ANALYZE TABLE listsquares COMPUTE STATISTICS FOR COLUMNS id, square

// COMMAND ----------

spark.table("biglist").join(spark.table("listsquares").filter('square < 10000), "id").join(spark.table("small"), "id").collect

// COMMAND ----------

// MAGIC %md Referring to the SQL UI, we can see that this change resulted in two improvements:
// MAGIC 
// MAGIC * The __join ordering is optimal and takes into account the `filter`__ that keeps just 100 rows of the squarelist table
// MAGIC * With the additional size data, __Spark avoids the Sort-Merge join and performs both joins as Broadcast__ joins 
// MAGIC 
// MAGIC *This is just a brief look at the CBO technology in Spark 2.2. For more details, look at the presentations from Spark Summit SF 2017:*
// MAGIC 
// MAGIC * Cost Based Optimizer in Apache Spark 2.2 (Part 1) - Ron Hu & Sameer Agarwal https://www.youtube.com/watch?v=qS_aS99TjCM
// MAGIC * Cost Based Optimizer in Apache Spark 2.2 (Part 2) - Zhenhua Wang & Wenchen Fan https://www.youtube.com/watch?v=8J-qffTYheE
// MAGIC * Slides accompanying those sessions - https://www.slideshare.net/databricks/costbased-optimizer-in-apache-spark-22
// MAGIC 
// MAGIC __Takeaway: Benchmarking Perf Improvements on TPC-DS__
// MAGIC * 16 queries show speedup > 30%
// MAGIC * Max speedup is 8x
// MAGIC * Geometric mean of speedup is 2.2x

// COMMAND ----------

// MAGIC %md #### Returning to the surface from our deep dive...
// MAGIC 
// MAGIC Let's try a practical business use case: we'll join our pageview data with some geography data and see whether big cities get searched more than small ones

// COMMAND ----------

// MAGIC %fs ls /mnt/spark-live

// COMMAND ----------

spark.read.json("/mnt/spark-live/zips.json").withColumnRenamed("_id", "zip").createOrReplaceTempView("zip")

// COMMAND ----------

// MAGIC %sql SELECT * FROM zip

// COMMAND ----------

// MAGIC %sql SELECT page, pop, requests FROM pv JOIN zip ON page = city ORDER BY requests DESC;

// COMMAND ----------

// MAGIC %md That works ... but ... business logic fail! New\_York (in Wikipedia) won't match NEW YORK in the zips dataset.
// MAGIC 
// MAGIC Let's see if there are some built-in funtions that might help:

// COMMAND ----------

// MAGIC %sql SHOW FUNCTIONS

// COMMAND ----------

// MAGIC %sql SELECT split(page, '_') FROM pv WHERE page LIKE 'New_%'

// COMMAND ----------

// MAGIC %md Ok, how about our report of Wikipedia requests for large cities:

// COMMAND ----------

// MAGIC %sql SELECT city, SUM(pop), SUM(requests) 
// MAGIC      FROM pv
// MAGIC      JOIN zip ON lower(page) = regexp_replace(lower(city), ' ', '_') 
// MAGIC      GROUP BY city, state 
// MAGIC      ORDER BY SUM(requests) DESC;

// COMMAND ----------

// MAGIC %md Something fishy is going on though: look at San Francisco's population ... that's way too high.
// MAGIC 
// MAGIC __Exercise__: Can you find the real population data in the zip table, and figure out what's going wrong?

// COMMAND ----------

// try it!

// COMMAND ----------

// MAGIC %md Now, if we really needed to, we *could* code our own functions as well:

// COMMAND ----------

val isOdd = spark.udf.register("isOdd", (n:Long) => n%2 == 1 )

// COMMAND ----------

val someNumbers = spark.range(20)

someNumbers.select('id, isOdd('id)).show

// COMMAND ----------

// MAGIC %sql SELECT id, isOdd(id) FROM range(10)

// COMMAND ----------

// MAGIC %md *Advanced Topic:* We can also look at rows -- including just parts of rows, or rows with embedded data structures! -- as a Scala type. This feature is called typed Dataset and allows you to leverage nearly all of the performance-enhancing features of Catalyst and Tungsten, while still having access to native Scala types when you need them.
// MAGIC 
// MAGIC Why? Sometimes we need to call our existing custom business logic (perhaps Java) or it's easier to express a query or aggregation in code:

// COMMAND ----------

case class Zip(city:String) {
  def isSpecial = {
    city contains "BACON" // complex, legacy, regulated, or otherwise special business logic!
  }
}

spark.table("zip").as[Zip].filter(_.isSpecial).show

// COMMAND ----------

case class Zip(city:String, state:String, zip:String)
display(
  spark.table("zip")
    .as[Zip].groupByKey(z => z.city + "|" + z.state)
    .reduceGroups( (z1, z2) => Zip(z1.city, z1.state, z1.zip + " " + z2.zip) )
)

// COMMAND ----------

// MAGIC %md #### RDD (Resilient Distributed Dataset)
// MAGIC 
// MAGIC This is a very brief intro to the older, lower-level data analysis approach.
// MAGIC 
// MAGIC The main goal is to contrast it to the modern, DataFrame-based method.

// COMMAND ----------

// MAGIC %md Let's start by reading the records -- each line is one records -- into a RDD[String] and then counting it.

// COMMAND ----------

val file = "/FileStore/pageviews-csv"
sc.textFile(file).count

// COMMAND ----------

// MAGIC %md Just for contrast, let's look at how we would answer the query "How many total views were served for pages like Apache\_\* (more or less Apache projects)?"
// MAGIC 
// MAGIC First we want to restrict to English:

// COMMAND ----------

sc.textFile(file)
    .map(line => line.split(","))
    .filter(line => (line(0) == "en")) // field "0" is the project column
    .count

// COMMAND ----------

// MAGIC %md Now let's do the filter for Apache\_

// COMMAND ----------

sc.textFile(file)
    .map(line => line.split(","))
    .filter(line => (line(0) == "en"))
    .filter(line => (line(1).toLowerCase.startsWith("apache_"))) // field "1" is the page column
    .count

// COMMAND ----------

// MAGIC %md So that's the number of distinct page entries ... now let's add up the views:

// COMMAND ----------

sc.textFile(file)
  .map(line => line.split(","))
  .filter(line => (line(0) == "en"))
  .filter(line => (line(1).toLowerCase.startsWith("apache_")))
  .map(_(2).toInt) // field "2" is requests, and we still have the string from the line.split...
  .reduce(_ + _)

// COMMAND ----------

// MAGIC %md Note some differences:
// MAGIC 
// MAGIC Basic differences:
// MAGIC * SQL is easier to read and write
// MAGIC * We had do our own parsing (`line.split(",")`) with RDDs. This was an easy case, but more complex cases would require more work and (hopefully) developing reusable modules
// MAGIC * We had to manage schema ourselves -- e.g., knowing that column 3 was an Int, but represented in our structure as a String, then casting it later
// MAGIC * The aggregation (reduce) was simple here because we were adding Ints to get an Int, but more complex calculations would require more complex aggregation calls ... GROUP BY would require calls to keyBy and reduce (or aggregate) by key for good performance
// MAGIC 
// MAGIC Subtle differences:
// MAGIC * Since all of our calls (map/filter/reduce) took Scala (Java) funtions, Spark needs to supply Scala/Java objects as parameters and receives the same as output.
// MAGIC   * This is intuitive and sensible for a JVM tool __but__ it's not optimal
// MAGIC * Wouldn't it be cool to pass only the columns needed for an operation?
// MAGIC   * How about optimizing by doing things like combining the filters?
// MAGIC   * Or sorting a column based on primitive values, rather than treating it as a sort on Java objects (all fields accessed by reference)
// MAGIC   
// MAGIC DataFrame / Dataset does all of these optimizations and more.

// COMMAND ----------

// MAGIC %md ##Machine Learning with Spark + Databricks

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC path = "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv"
// MAGIC 
// MAGIC spark.read.csv(path).show()

// COMMAND ----------

// MAGIC %md We can use the header data as column names through the regular reader option call:

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC spark.read.option("header", True).csv(path).show()

// COMMAND ----------

// MAGIC %md In this example, we're just going to look at carat weight as a linear predictor of price. So we'll grab those columns and use the inferSchema option to get numbers (recall the original data is a CSV file which defaults to reading strings):

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC data = spark.read.option("header", True) \
// MAGIC             .option("inferSchema", True) \
// MAGIC             .csv(path) \
// MAGIC             .select("carat", "price")

// COMMAND ----------

// MAGIC %md Let's have a quick look to see if there's any hope for our linear regression:

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC display(data.sample(False, 0.01))

// COMMAND ----------

// MAGIC %md What's the Pearson correlation?

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC data.stat.corr("price", "carat")

// COMMAND ----------

// MAGIC %md Ok, so there is a chance we'll get something useful out... 
// MAGIC 
// MAGIC But what do we need to feed in?
// MAGIC 
// MAGIC Vectors!
// MAGIC 
// MAGIC The training (and validation, test, etc.) data needs to be collected into a column of Vector[Double]
// MAGIC 
// MAGIC We could do this conversion ourselves, by writing a UDF, but luckily, there's a built-in `Transformer` that will take one or more columns of numbers and collect them into a new column containing a Vector[Double]

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC from pyspark.ml.feature import *
// MAGIC 
// MAGIC assembler = VectorAssembler(inputCols=["carat"], outputCol="features")

// COMMAND ----------

// MAGIC %md First look at the next cell, to help make it concrete what this transformer does, and what transformers do in general.
// MAGIC 
// MAGIC Next, take a look at the VectorAssembler docs and maybe even the source code.

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC assembler.transform(data).show()

// COMMAND ----------

// MAGIC %md Now we'll add the `LinearRegression` algorithm. The algorithm builds a model from data.
// MAGIC 
// MAGIC Since it needs to look at all the data and then build a new piece of state (representing the `Model`) that can be used for predictions on each row (a Model is a subtype of `Transformer`), it is an `Estimator`.

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC from pyspark.ml.regression import *
// MAGIC 
// MAGIC lr = LinearRegression(labelCol="price")

// COMMAND ----------

// MAGIC %md We can operate each of these components separately, to see how they're working (but we'll see a shortcut in just a minute)

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC train, test = data.randomSplit([0.75, 0.25])
// MAGIC 
// MAGIC lrModel = lr.fit ( assembler.transform(train) )
// MAGIC 
// MAGIC lrModel.transform( assembler.transform(test) ).show()

// COMMAND ----------

// MAGIC %md Let's package the processing steps together so that we don't need to run them
// MAGIC * separately
// MAGIC * for training, validation sets, etc.
// MAGIC 
// MAGIC The `Pipeline` is an `Estimator` that represents composing a series of `Transformer`s or `Estimator`-`Model` pairs.
// MAGIC 
// MAGIC When we add an `Estimator` to a pipeline (without specifically fitting the `Estimator` first), we are performing composition -- `Pipeline`s are themselves `Estimator`s, so we're making a new `Estimator` that includes the `LinearRegression` algorithm as a component part.

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC from pyspark.ml import Pipeline
// MAGIC 
// MAGIC pipeline = Pipeline(stages=[assembler, lr])

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC model = pipeline.fit(train)

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC model.stages[-1].hasSummary

// COMMAND ----------

// MAGIC %md Note this is a summary of the model, not a summary of the test. So it's showing training error, or "apparent error."

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC summary = model.stages[-1].summary
// MAGIC print(summary.r2)
// MAGIC print(summary.rootMeanSquaredError)

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC display(summary.residuals.sample(False, 0.05)) # training residuals

// COMMAND ----------

// MAGIC %md Now ... how did we do on test data?

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC predictions = model.transform(test)

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC display(predictions.sample(False, 0.02).selectExpr("prediction - price as error"))

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC from pyspark.ml.evaluation import *
// MAGIC 
// MAGIC eval = RegressionEvaluator(labelCol="price", predictionCol="prediction")

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC eval.evaluate(predictions)

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC for line in eval.explainParams().split("\n"):
// MAGIC   print(line)

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC help(eval)

// COMMAND ----------

// MAGIC %md It looks like we did about as well (or badly) on the test data as on the training data.

// COMMAND ----------

// MAGIC %md Last, let's extract the parameters of the model:

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC model.stages[-1].intercept

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC model.stages[-1].coefficients

// COMMAND ----------

// MAGIC %md Recap... What have we looked and not looked at?
// MAGIC 
// MAGIC Looked at:
// MAGIC * Basic data preparation (types, DataFrame, vectors)
// MAGIC * Feature pre-processing helper example: VectorAssembler
// MAGIC * Role and type of a Transformer
// MAGIC * Model-building algorithm example: LinearRegression
// MAGIC * Role and type of an Estimator
// MAGIC * Pipeline
// MAGIC * Some basic graphs and statistics along the way
// MAGIC 
// MAGIC Have *not* looked at:
// MAGIC * Cleaning, deskewing, other data pre-processing
// MAGIC * Various data prep helpers
// MAGIC * Other algorithms
// MAGIC * Model tuning and cross-validation
// MAGIC * Combining Spark with other models and tools (sklearn, deep learning, etc.)
// MAGIC * Data-parallelism strategy

// COMMAND ----------

// MAGIC %md One-variable linear regression? Really? Can't we do something a tiny bit fancier?
// MAGIC 
// MAGIC For the data scientists, we'll take a quick look at a more powerful model -- a gradient-boosted tree ensemble -- using all of the features in the dataset:

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC categoricalFields = ["cut", "color", "clarity"]
// MAGIC 
// MAGIC indexers = [StringIndexer(inputCol=f, outputCol=f + "Index") for f in categoricalFields]
// MAGIC 
// MAGIC assembler = VectorAssembler( \
// MAGIC   inputCols=[f + "Index" for f in categoricalFields] + ["carat", "depth", "table", "x", "y", "z"], \
// MAGIC   outputCol="features")
// MAGIC 
// MAGIC gbt = GBTRegressor(labelCol="price")
// MAGIC gbtPipeline = Pipeline(stages=indexers + [assembler, gbt])
// MAGIC 
// MAGIC allFields = spark.read.option("header", True).option("inferSchema", True).csv(path)
// MAGIC train, test = allFields.randomSplit([0.75, 0.25])
// MAGIC 
// MAGIC gbtModel = gbtPipeline.fit(train)
// MAGIC predictions = gbtModel.transform(test)
// MAGIC eval.evaluate(predictions)

// COMMAND ----------

// MAGIC %md ####What about Deep Learning?
// MAGIC 
// MAGIC __Training Full Models in Spark__
// MAGIC   * Intel BigDL - CPU focus
// MAGIC   * DeepLearning4J - GPU support
// MAGIC   * dist-keras - Keras API + distributed research-grade algorithms + GPU
// MAGIC   * TensorFlowOnSpark
// MAGIC   
// MAGIC __Transfer Learning__ (E.g., Neural Net as Featurizer + Spark Classifier)
// MAGIC   * Databricks - Spark Deep Learning Pipelines
// MAGIC   * Microsoft - MMLSpark
// MAGIC   
// MAGIC __Bulk Inference in Spark__
// MAGIC   * All of the above  

// COMMAND ----------

// MAGIC %md #### What About Fast Prediction on Just a Few Vectors?
// MAGIC 
// MAGIC For low-latency (< ~500ms) inference on one or a small set of vectors, you may want to look at complementary tools.
// MAGIC 
// MAGIC Libraries can take a Spark pipeline or model and allow it to be deployed outside of Spark in a lightweight scale-out service:
// MAGIC * Databricks "dbml-local" (Databricks customers, common ML models)
// MAGIC * MLeap (http://mleap-docs.combust.ml/) for Spark ML Pipelines, Tensorflow, and Scikit-Learn
// MAGIC * a few other players...

// COMMAND ----------

// MAGIC %md ##Structured Streaming
// MAGIC 
// MAGIC Apache Spark Structured Streaming is now production ready! Let's use it to take a look at live Wikipedia edits.
// MAGIC 
// MAGIC We can read them in JSON structures from a server/port:

// COMMAND ----------

// MAGIC %sh timeout 1 nc 54.213.33.240 9002

// COMMAND ----------

// MAGIC %sql SET spark.sql.shuffle.partitions = 3 
// MAGIC -- fewer tasks for small volume stream

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import spark.implicits._

// COMMAND ----------

// MAGIC %md Discuss...
// MAGIC 
// MAGIC Demo sources and sinks vs. real ones
// MAGIC 
// MAGIC Let's start by just parsing the data and creating a StreamingQuery:

// COMMAND ----------

val lines = spark.readStream
  .format("socket")
  .option("host", "54.213.33.240")
  .option("port", 9002)
  .load()

val edits = lines.select(json_tuple('value, "channel", "timestamp", "isRobot", "isAnonymous"))

// COMMAND ----------

display(edits)

// COMMAND ----------

// MAGIC %md We'll try something a little more complex.
// MAGIC 
// MAGIC * Rename the columns something more useful than c0, c1, etc.
// MAGIC * Interpret the time as SQL timestamp (it was a raw string earlier)
// MAGIC * Create 10-second windows over the timestamp
// MAGIC * Transform the stream by grouping by channel and time, then counting edits

// COMMAND ----------

val lines = spark.readStream
  .format("socket")
  .option("host", "54.213.33.240")
  .option("port", 9002)
  .load()

val edits = lines
                .select(json_tuple('value, "channel", "timestamp", "page"))
                .selectExpr("c0 as channel", "cast(c1 as timestamp) as time", "c2 as page")
                .createOrReplaceTempView("edits")

val editCounts = spark.sql("""SELECT count(*), channel, date_format(window(time, '10 seconds').start, 'HH:mm:ss') as time 
                              FROM edits 
                              GROUP BY channel, window(time, '10 seconds')
                              ORDER BY time""")

// COMMAND ----------

display(editCounts)

// COMMAND ----------

// MAGIC %md There's a lot of magic going on in that example!
// MAGIC 
// MAGIC Consider what happens when a record comes in late ... meaning it has a timestamp from a few seconds earlier.
// MAGIC 
// MAGIC The `GROUP BY` means that this data needs to be aggregated with the *previously received records having a corresponding timestamp*
// MAGIC 
// MAGIC In other words, the data need to be processed based on their indicated order not their received order, and that means *changing an aggregation that was already reported to the output sink*
// MAGIC 
// MAGIC The important takeaways in this overview are that
// MAGIC 
// MAGIC * Spark is designed to do exactly this, and to do it in an optimized and fault-tolerant fashion
// MAGIC * The performance or suitability of this approach depends on where the data needs to go (the sink)

// COMMAND ----------

// MAGIC %md #####What do we need to do to leverage the most powerful included features in a production environment?
// MAGIC E.g.,
// MAGIC * Fault tolerance
// MAGIC * Available source/sink strategies
// MAGIC * Incremental query optimization
// MAGIC 
// MAGIC https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
// MAGIC 
// MAGIC Adding a library to Spark on Databricks is easy -- start with Create -> Library in the file GUI. We can look for `spark-sql-kafka-*` ... although in our current Databricks environment, Structured Streaming Kafka access is preloaded.

// COMMAND ----------

// Reading from Kafka returns a DataFrame with the following fields:
//
// key           - data key (i.e., for key-value records; we aren't using it here)
// value         - data, in base64 encoded binary format. This is our JSON payload. We'll need to cast it to STRING.
// topic         - Kafka topic. In this case, the topic is the same as the "wikipedia" field, so we don't need it.
// partition     - Kafka topic partition. This server only has one partition, so we don't need this information.
// offset        - Kafka topic-partition offset value -- this is essentially the message's unique ID. Take a look at the values Kafka produces here!
// timestamp     - not used
// timestampType - not used

val kafkaDF = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "54.213.33.240:9092")
  .option("subscribe", "en,ru,zh,pl,de")
  .load()

val editsDF = kafkaDF.select($"value".cast("string").as("value"))

// COMMAND ----------

display(editsDF)

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

val schema = StructType(List(
                StructField("wikipedia", StringType, false),
                StructField("timestamp", TimestampType, false),
                StructField("page", StringType, false),
                StructField("isRobot", BooleanType, false)
             ))

val extractedDF = editsDF.select(from_json('value, schema) as "json")

// COMMAND ----------

display(extractedDF)

// COMMAND ----------

extractedDF.printSchema

// COMMAND ----------

val windowColDF = extractedDF.select($"json.wikipedia", window($"json.timestamp", "10 seconds") as "time", $"json.page", $"json.isRobot")

// COMMAND ----------

val prettyView = windowColDF.groupBy('time, 'wikipedia)
                            .count()
                            .withColumn("period", date_format($"time.start", "HH:mm:ss"))
                            .orderBy("period", "wikipedia")

display(prettyView)

// COMMAND ----------

dbutils.fs.rm("/tmp/demo", true)
dbutils.fs.rm("/tmp/ck", true)

val query = windowColDF.writeStream
  .option("checkpointLocation", "/tmp/ck")
  .format("parquet")
  .start("/tmp/demo")

// COMMAND ----------

val prettyView = spark.read.parquet("/tmp/demo")
                            .groupBy('time, 'wikipedia)
                            .count()
                            .withColumn("period", date_format($"time.start", "HH:mm:ss"))
                            .orderBy("period", "wikipedia")

display(prettyView)

// COMMAND ----------

// MAGIC %md This approach is the future of Spark streaming, making continuous applications much easier to engineer and more flexible toward changing business requirements.

// COMMAND ----------

query.stop

// COMMAND ----------

// MAGIC %md ### Where to learn more?
// MAGIC 
// MAGIC There's lots of material out there -- make sure you're learning about __modern Spark (2.x)__ and modern best practices!
// MAGIC 
// MAGIC Luckily, we now have those books starting to come out:
// MAGIC 
// MAGIC <table style="border:none"><tr><td style="border:none">
// MAGIC <img src="https://i.imgur.com/LwLmsvX.png" width=300>
// MAGIC <img src="https://i.imgur.com/PRMBvHu.png" width=300>
// MAGIC <img src="https://i.imgur.com/AkmQ5az.png" width=300>
// MAGIC </td></tr></table>

// COMMAND ----------

