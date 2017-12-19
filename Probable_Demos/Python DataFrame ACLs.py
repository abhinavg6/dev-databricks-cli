# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Fine-grained Data Access Control for Python DataFrames
# MAGIC 
# MAGIC *Available in DBR-3.4 or later.*
# MAGIC 
# MAGIC Controlling data access is always a challenge in data driven companies. Databricks is proud to extend our SQL ACLs to the world of Python. This will allow you to do two things.
# MAGIC 
# MAGIC 1. To set up a cluster that has fine grained security over objects while still providing the power of Python. We do this by only allowing Python to call APIs that respect our SQL ACLs.
# MAGIC 2. To setup a cluster with strong user isolation, even for Python users. This protects against users accidentally or intentionally trying to overwrite or access data, providing the same level of isolation as if those users were on separate clusters.
# MAGIC 
# MAGIC Let's go ahead and see how all of this actually works. You'll want to clone this notebook (or import it) and grab a friend to play around with access controls.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC # Creating a Cluster with Python DataFrame ACLs enabled
# MAGIC 
# MAGIC 
# MAGIC Start a cluster running DBR 3.4 or later* with the following configuration:
# MAGIC 
# MAGIC ```
# MAGIC spark.databricks.acl.enabled true
# MAGIC spark.databricks.pyspark.enableExecutorFsPerms true
# MAGIC spark.databricks.pyspark.useFileBasedCollect true
# MAGIC spark.databricks.pyspark.runAsLowPrivilegeUser true
# MAGIC spark.databricks.pyspark.enableIptables true
# MAGIC spark.databricks.pyspark.enablePy4JSecurity true
# MAGIC spark.databricks.repl.allowedLanguages sql,python
# MAGIC ```
# MAGIC 
# MAGIC // TODO(greg): remove this line once DBR 3.4 is released
# MAGIC 
# MAGIC *If DBR 3.4 is not yet available on your shard, use custom:spark-image-34874518e41cb395830cf656bbdc149815676c74a2f3def099cde5dd59719d3a
# MAGIC 
# MAGIC The first configuration will enable *fine-grained access control* on that cluster, the next five all prevent different methods of circtumventing that access control, and the final configuration will ensure that only SQL and Python can be run on the cluster. This means that using commands like `%scala` or `%r` will fail. For instance, we can see this in the cell below when I ran it on such a cluster.

# COMMAND ----------

#%scala

#println("HELLO WORLD")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Once you create this cluster, we can go about managing the access control of different objects on the cluster. Data access control in Databricks revolves around three core concepts.
# MAGIC 
# MAGIC 1. *Users* - these refer directly to your Databricks users and those that will be executing commands on the cluster. There are effectively two levels of users, administrators and regular users.
# MAGIC 2. *Objects* - these refer to the "objects" in SQL: `CATALOG`, `DATABASE`, `TABLE`, `VIEW` and `FUNCTION`.
# MAGIC 3. *Privileges* - these determine what a user can do on an object.
# MAGIC 
# MAGIC In simplest terms, the workflow is - an *administrator* grants a given *user* certain *privileges* to certain *objects*. These privileges, which are checked by both the SQL API and the Python DataFrame API, determine what a user can and cannot do with a given object. Let's work through a simple example before going through the details.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Creating Access Controlled Resources
# MAGIC 
# MAGIC By default, non-administrative users will not have access to any objects. We can see this if we query the access control for a given object. In this case we will query the default database.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SHOW GRANT ON DATABASE default

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC There has been no explicit granting of permissions at this point so there may not be any data in this table. Let's create a simple table to explore setting permissions.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS acl_demo_table;
# MAGIC -- the above just makes sure this demo table doesn't exist
# MAGIC CREATE TABLE acl_demo_table AS 
# MAGIC   SELECT 1 as col1, 2 as col2;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now we can see the permissions on that specific table, nothing will have changed at this point but we can always check. Notice how we have seen two different ACL controls, one for a `DATABASE` and one for a `TABLE`.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SHOW GRANT ON TABLE acl_demo_table

# COMMAND ----------

# MAGIC %md
# MAGIC Now at this point none of my users have access to this data, but I do. That's because I am the *owner* of this table because I created it.
# MAGIC 
# MAGIC We can show that the Python DataFrame API respects these SQL ACLs if we run the following command as a different user:

# COMMAND ----------

# as coworker
df = spark.read.table("acl_demo_table")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Granting Access to a Resource
# MAGIC 
# MAGIC We see that users cannot use the Python DataFrame API to access tables on which they have no permissions.
# MAGIC 
# MAGIC Now I want to enable my coworker to be able to query this table. I'm going to give her appropriate access to do so.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC GRANT SELECT ON acl_demo_table to `abhinav.garg+11@databricks.com`;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now she can run select statements against that table. We can see that when we look at her permissions.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SHOW GRANT ON TABLE acl_demo_table

# COMMAND ----------

# as coworker
df = spark.read.table("acl_demo_table")
df.write.mode("overwrite").csv("/tmp/acl_demo_table")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC For more information on the fine-grained controls available via SQL ACLs, see that demo notebook

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Preventing users from accessing data through unsupervised APIs
# MAGIC 
# MAGIC While the Python DataFrame API respects SQL ACLs, other APIs do not. For example, the RDD API attempts to read files directly from disk, without checking the ACLs on the tables that reference those files. To prevent this unwanted access, we prohibit users from calling the RDD API.

# COMMAND ----------

sc.textFile("/tmp/acl_demo_table")

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Access to the filesystem on the driver via %sh is still allowed, but each REPL runs commands as a different user with restricted privileges.

# COMMAND ----------

# MAGIC %sh whoami

# COMMAND ----------

# MAGIC %md
# MAGIC For example, we cannot assume another user's identity

# COMMAND ----------

# MAGIC %sh runuser -l ubuntu -c 'ls /home/ubuntu'

# COMMAND ----------

# MAGIC %md
# MAGIC Nor can we look at the Spark logs, which might give away the commands being run by other users

# COMMAND ----------

# MAGIC %sh ls -lct /databricks/spark/logs

# COMMAND ----------

# MAGIC %md
# MAGIC We also prevent direct access to data stored in the cloud, both by restricting access to credentials files...

# COMMAND ----------

# MAGIC %sh cat /databricks/hive/conf/hive-site.xml

# COMMAND ----------

# MAGIC %md
# MAGIC ...and by preventing users from establishing connections to cloud provider metadata services

# COMMAND ----------

# MAGIC %sh curl http://169.254.169.254/latest/meta-data/

# COMMAND ----------

# MAGIC %md
# MAGIC We also prevent access to disk files created by Spark (for example, during operations that spill to disk)

# COMMAND ----------

# MAGIC %sh ls -lct /local_disk0

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Preserving the power of Python
# MAGIC 
# MAGIC Despite all these restrictions, users are still able to take advantage of Python's expressiveness and rich environment of third-party libraries

# COMMAND ----------

# TODO: beforehand, install Flask from Pypi
from flask import Flask

# COMMAND ----------

