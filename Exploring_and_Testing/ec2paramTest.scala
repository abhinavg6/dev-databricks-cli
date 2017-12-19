// Databricks notebook source
import com.amazonaws.services.simplesystemsmanagement._

// COMMAND ----------

val sSMClient = new AWSSimpleSystemsManagementClient()

// COMMAND ----------

val param = new com.amazonaws.services.simplesystemsmanagement.model.GetParametersRequest()
param.withNames("kp-api-test")
param.withWithDecryption(false)

// COMMAND ----------

val test = sSMClient.getParameters(param)

// COMMAND ----------

val test2 = test.getParameters.get(0).getValue

// COMMAND ----------

