// Databricks notebook source
val data = spark
  .read
  .option("InferSchema","true")
  .option("header","true")
  .option("delimiter","\t")
  .csv("/FileStore/tables/Datos_ENEMDU_PEA_v2.csv")

// COMMAND ----------

// DBTITLE 1,Leer el archivo con los datos de provincias
val provinces = spark
  .read
  .option("InferSchema","true")
  .option("header","true")
  .option("delimiter","\t")
  .csv("/FileStore/tables/provincias.csv")

// COMMAND ----------

provinces.show(false)

// COMMAND ----------

// DBTITLE 1,Función inner Join para unir las tablas
val Innerprovinces = data.join(provinces, data("provincia") === provinces("codigoprov"), "inner")

// COMMAND ----------

display(Innerprovinces.select("provincia","codigoprov","nombreprov").orderBy($"provincia").distinct)

// COMMAND ----------

// DBTITLE 1,Borrar las columnas que esta de más
val dataf = Innerprovinces.drop("provincia").drop("codigoprov")


// COMMAND ----------

dataf.show

// COMMAND ----------


