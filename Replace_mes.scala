// Databricks notebook source
val data = spark
  .read
  .option("InferSchema","true")
  .option("header","true")
  .option("delimiter","\t")
  .csv("/FileStore/tables/Datos_ENEMDU_PEA_v2.csv")

// COMMAND ----------

val mes = spark
  .read
  .option("InferSchema","true")
  .option("header","true")
  .option("delimiter","\t")
  .csv("/FileStore/tables/mes.csv")

// COMMAND ----------

mes.show(false)

// COMMAND ----------

val Innermes = data.join(mes, data("mes") === mes("idmes"), "inner")

// COMMAND ----------

display(Innermes.select("mes","idmes","nombreMes").orderBy($"mes").distinct)

// COMMAND ----------

val dataf = Innermes.drop("mes").drop("idmes")

// COMMAND ----------

dataf.show

// COMMAND ----------


