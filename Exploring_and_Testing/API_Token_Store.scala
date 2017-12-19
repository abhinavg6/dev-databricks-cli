// Databricks notebook source
package my.databricks.abhinav

object SecretStore {
  var token = "Init"
  
  def setToken(): Unit = {
    token = "dapi5afb635f0beb3978a8376b487803ba96"
  }
  
  def getToken(): String = token
}

// COMMAND ----------

import my.databricks.abhinav.SecretStore

SecretStore.setToken()

// COMMAND ----------

import my.databricks.abhinav.SecretStore

dbutils.notebook.exit(SecretStore.getToken())