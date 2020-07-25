// Databricks notebook source
val data = spark
  .read
  .option("InferSchema","true")
  .option("header","true")
  .option("delimiter","\t")
  .csv("/FileStore/tables/Datos_ENEMDU_PEA_v2.csv")

// COMMAND ----------


