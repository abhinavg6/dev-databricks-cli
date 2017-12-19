// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC # Table Access Control and SQL Only Clusters
// MAGIC 
// MAGIC *Available in DBR-3.0 or later.*
// MAGIC 
// MAGIC Controlling data access is always a challenge in data driven companies. Databricks is proud to introduce SQL Only clusters and SQL object access controls. This will allow you to do two things.
// MAGIC 
// MAGIC 1. To set up a cluster that has fine grained security over objects. This is done by preventing arbitrary code from running on the cluster by only enabling you to run SQL.
// MAGIC 2. To setup a cluster with access controls enabled, this prevents users from accidentally or intentionally trying to overwrite or access data.
// MAGIC 
// MAGIC Let's go ahead and see how all of this actually works. You'll want to clone this notebook (or import it) and grab a friend to play around with access controls.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC 
// MAGIC # Creating a SQL Enabled Cluster
// MAGIC 
// MAGIC 
// MAGIC start a cluster with the following configuration which will set up two different things.
// MAGIC 
// MAGIC ```
// MAGIC spark.databricks.acl.sqlOnly true
// MAGIC ```
// MAGIC 
// MAGIC This will enable *fine-grained access control* on that cluster, the second configuration will ensure that only SQL can be run on a given cluster. This means that using commands like `%scala` or `%python` will fail with the error "Only SQL commands are allowed on an ACL enabled cluster". For instance, we can see this in the cell below when I ran it on such a cluster.

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC print "HELLO WORLD"

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Once you create this cluster, we can go about managing the access control of different objects on the cluster. SQL access control in Databricks revolves around three core concepts.
// MAGIC 
// MAGIC 1. *Users* - these refer directly to your Databricks users and those that will be executing commands on the cluster. There are effectively two levels of users, administrators and regular users.
// MAGIC 2. *Objects* - these refer to the "objects" in SQL: `CATALOG`, `DATABASE`, `TABLE`, `VIEW` and `FUNCTION`.
// MAGIC 3. *Privileges* - these determine what a user can do on an object.
// MAGIC 
// MAGIC In simplest terms, the workflow is - an *administrator* grants a given *user* certain *privileges* to certain *objects*. These privileges determine what a user can and cannot do with a given object. Let's work through a simple example before going through the details.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Creating Access Controlled Resources
// MAGIC 
// MAGIC By default, non-administrative users will not have access to any objects. We can see this if we query the access control for a given object. In this case we will query the default database.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SHOW GRANT ON DATABASE default

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC There has been no explicit granting of permissions at this point so there may not be any data in this table. Let's create a simple table to explore setting permissions.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC DROP TABLE IF EXISTS acl_demo_table;
// MAGIC -- the above just makes sure this
// MAGIC -- demo table doesn't exist

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE acl_demo_table AS 
// MAGIC   SELECT 1 as col1, 2 as col2;

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Now we can see the permissions on that specific table, nothing will have changed at this point but we can always check. Notice how we have seen two different ACL controls, one for a `DATABASE` and one for a `TABLE`.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SHOW GRANT ON TABLE acl_demo_table

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Now at this point none of my users have access to this data, but I do. That's because I am the *owner* of this table because I created it.
// MAGIC 
// MAGIC # Granting Access to a Resource
// MAGIC 
// MAGIC Now I want to enable my coworker to be able to query this table. I'm going to give her appropriate access to do so.
// MAGIC * Create a new user if required to test this (Created alias user abhinav.garg+11@databricks.com)

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC GRANT SELECT ON acl_demo_table to `abhinav.garg+11@databricks.com`;

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Now she can run select statements against that table. We can see that when we look at her permissions.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SHOW GRANT ON TABLE acl_demo_table

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC -- as coworker
// MAGIC SELECT * FROM acl_demo_table

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC I can also give her access to an entire database.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE DATABASE IF NOT EXISTS my_acled_db

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SHOW GRANT ON DATABASE my_acled_db

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC GRANT ALL PRIVILEGES ON DATABASE my_acled_db to `abhinav.garg+11@databricks.com`;

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC This will give her `SELECT` access to all tables and views that exist in that database. We could of course, give her more access if necessary by giving other privileges on this database.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Fine-Grained Security (Column and Row Based)
// MAGIC 
// MAGIC Now let's say that over the course of my work, I create a great manipulation of this dataset that only includes a single column. I can save that as a *view* on my current table because it's just a logical transformation.
// MAGIC * Create a new user if required to test this (Created alias user abhinav.garg+22@databricks.com)

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE OR REPLACE VIEW my_wonderful_view AS
// MAGIC   SELECT col1 FROM acl_demo_table
// MAGIC   WHERE col2 = 2

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC By default no one but myself will have access to this view.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SHOW GRANT ON VIEW my_wonderful_view

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC But I might want to share it.
// MAGIC 
// MAGIC Create a new group using REST API
// MAGIC ```
// MAGIC curl -X POST -H 'Content-Type: application/json' -H 'Authorization: Bearer xxxyyy' -d '{ "group_name": "View_Readers_Only" }' https://field-eng.cloud.databricks.com/api/2.0/groups/create
// MAGIC ```
// MAGIC 
// MAGIC Add user to the new group
// MAGIC ```
// MAGIC curl -X POST -H 'Content-Type: application/json' -H 'Authorization: Bearer xxxyyy' -d '{ "user_name": "abhinav.garg+22@databricks.com", "parent_name": "View_Readers_Only" }' https://field-eng.cloud.databricks.com/api/2.0/groups/add-member
// MAGIC ```

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC GRANT SELECT ON my_wonderful_view to `View_Readers_Only`

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Now a second coworker has access to this view as well. You'll notice that we have now set up *fine-grained* access control in Spark SQL. I can limit access to both rows and columns and making it easy to customize exactly what every user has access to.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SHOW GRANT ON VIEW my_wonderful_view

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT * from my_wonderful_view

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Now however, I realized that the second coworker should no longer have access to this given resources. I can now revoke her permissions to a specific table, or all resources that she has access to.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC REVOKE ALL PRIVILEGES ON VIEW my_wonderful_view FROM `View_Readers_Only`

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SHOW GRANT ON VIEW my_wonderful_view

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Securing Unmanaged tables
// MAGIC 
// MAGIC One important note is the concept of *managed* vs *unmanaged* tables. Tables store two important pieces of information. The data within the tables as well as the data about the tables, that is the *metadata*. You can have Spark manage the metadata for a set of files, as well as the data. When you define a table from files on disk, you are defining an unmanaged table. When you use `saveAsTable` on a `DataFrame` you are creating a managed table where Spark will keep track of all of the relevant information for you. By default Spark stores this information in the `/user/hive/warehouse` directory.
// MAGIC 
// MAGIC Below I will create an *unmanaged table*. Spark will manage the *metadata* about this table however, the files/data are not managed by Spark. We create this table with the `CREATE EXTERNAL TABLE` statement.
// MAGIC 
// MAGIC ```sql
// MAGIC CREATE EXTERNAL TABLE external_table (
// MAGIC   DEST_COUNTRY_NAME STRING,
// MAGIC   ORIGIN_COUNTRY_NAME STRING,
// MAGIC   count LONG)
// MAGIC ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
// MAGIC LOCATION '/some/data/location'
// MAGIC ```
// MAGIC 
// MAGIC The same concepts apply to managed or unmanaged tables. We can set access controls on them and restrict users from doing any of the above.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Notes on Security and Implementation

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC As mentioned, a table in Spark consists of two things: *metadata*, the "logical" part of a table; and *data*, the "physical" part of a table. SQL access controls in Databricks protect the "logical" part of tables. Because SQL on Spark allows only logical manipulation, this makes SQL only clusters *strictly secure*. Only those with the ability to create tables can access the raw files and *potentially* overwrite them.
// MAGIC 
// MAGIC If we are not on a SQL only cluster how you go about granting access to files ends up being of greater consequence. The same "logical" protection applies for DataFrames, Datasets, and Spark SQL. However, the actual physical files on the other hand will not be protected from malicious or users that actively try and gain access to the raw files because users can execute arbitrary code, discover these locations, and/or use external libraries to access them.
// MAGIC 
// MAGIC Therefore we suggest you ensure that the users (and their keys and IAM roles) that can write data are distinct from those that can read data. This will prevent users from accidentally (or intentionally) overwriting directories that should not be overwritten.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Full documentation can be found at : https://gist.github.com/sameeragarwal/46716a69308bb2ddda225bdc926b0236

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC DROP VIEW my_wonderful_view

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC DROP TABLE acl_demo_table

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC DROP DATABASE my_acled_db

// COMMAND ----------

