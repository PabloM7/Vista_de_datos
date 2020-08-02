// Databricks notebook source
val data = spark
  .read
  .option("InferSchema","true")
  .option("header","true")
  .option("delimiter","\t")
  .csv("/FileStore/tables/Datos_ENEMDU_PEA_v2.csv")

// COMMAND ----------

import org.apache.spark.sql.types._
  val myDataSchema = StructType(
  Array(
        StructField("id", IntegerType, true),
        StructField("anio", IntegerType, true),
        StructField("mes", IntegerType, true),
        StructField("provincia", IntegerType, true),
        StructField("canton", IntegerType, true),
        StructField("area", StringType, true),
        StructField("genero", StringType, true),
        StructField("edad", IntegerType, true),
        StructField("estado", StringType, true),
        StructField("nivel_de_instruccion", StringType, true),
        StructField("etnia", StringType, true),
        StructField("ingreso_laboral", IntegerType, true),
        StructField("condicion_actividad", StringType, true),
        StructField("sectorizacion", StringType, true),
        StructField("grupo_ocupacion", StringType, true),
        StructField("rama_actividad", StringType, true),
        StructField("factor_expansion", DoubleType, true)
      ));

// COMMAND ----------

val data = spark
  .read
  .schema(myDataSchema)
  .option("InferSchema","true")
  .option("header","true")
  .option("delimiter","\t")
  .csv("/FileStore/tables/Datos_ENEMDU_PEA_v2.csv")

// COMMAND ----------

data.show

// COMMAND ----------

// DBTITLE 1,Ejemplo CrossTab - Clasificación de empleos por genero.
data.stat.crosstab("condicion_actividad","genero").orderBy("condicion_actividad_genero").show(false)

// COMMAND ----------

val empleosDF = data.stat.crosstab("condicion_actividad","genero").orderBy("condicion_actividad_genero")

// COMMAND ----------

// DBTITLE 1,CrossTab - Segunda forma de Presentar
empleosDF.show(false)

// COMMAND ----------

// DBTITLE 1,Pivot - Edad minima en Ecuador para trabajar
data.groupBy("condicion_actividad").pivot("genero").min("edad").orderBy("condicion_actividad").show(false)

// COMMAND ----------

// DBTITLE 1,Pivot
data.groupBy("condicion_actividad").pivot("genero").max("edad").orderBy("condicion_actividad").show(false)

// COMMAND ----------

// DBTITLE 1,Ingreso máximo por etnias y tipo de empleo.
data.groupBy("condicion_actividad").pivot("etnia").max("ingreso_laboral").orderBy("condicion_actividad").show(false)

// COMMAND ----------

// DBTITLE 1,Frecuencia de tipos de empleo por provincias del Ecuador
data.groupBy("condicion_actividad").pivot("provincia").count.show(false)

// COMMAND ----------

// DBTITLE 1,Estado civil relacionado con el tipo de empleo
data.groupBy("condicion_actividad").pivot("estado").count.show(false)

// COMMAND ----------

// DBTITLE 1,Promedio de edades referentes a los tipos de empleo por etnias
import org.apache.spark.sql.functions._
data.groupBy("condicion_actividad").pivot("etnia").agg(round(avg("edad"))).orderBy("condicion_actividad").show(false)
