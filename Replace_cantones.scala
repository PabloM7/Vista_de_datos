// Databricks notebook source
val data = spark
  .read
  .option("InferSchema","true")
  .option("header","true")
  .option("delimiter","\t")
  .csv("/FileStore/tables/Datos_ENEMDU_PEA_v2.csv")

// COMMAND ----------

val cantones = spark
  .read
  .option("InferSchema","true")
  .option("header","true")
  .option("delimiter","\t")
  .csv("/FileStore/tables/cantones.csv")

// COMMAND ----------

cantones.show

// COMMAND ----------

val Innercantones = data.join(cantones, data("canton") === cantones("idcanton"), "inner")

// COMMAND ----------

display(Innercantones.select("canton","idcanton","nombrecanton").orderBy($"canton").distinct)

// COMMAND ----------

val dataf = Innercantones.drop("canton").drop("idcanton")

// COMMAND ----------

dataf.show

// COMMAND ----------


