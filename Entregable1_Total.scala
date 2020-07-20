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

// DBTITLE 1,Condiciones actividades.
data.select("condicion_actividad").distinct().show

// COMMAND ----------

// DBTITLE 1,Data Empleo adecuado.
val EA= data.where($"condicion_actividad"==="1 - Empleo Adecuado/Pleno")
println(EA.count)

// COMMAND ----------

// DBTITLE 1,Data Empleo no clasificado
val ENC= data.where($"condicion_actividad"==="6 - Empleo no clasificado")
println(ENC.count)

// COMMAND ----------

// DBTITLE 1,Subempleo por insuficiencia de tiempo de trabajo
val SIT= data.where($"condicion_actividad"==="2 - Subempleo por insuficiencia de tiempo de trabajo")
println(SIT.count)

// COMMAND ----------

// DBTITLE 1,Desempleo oculto
val DO= data.where($"condicion_actividad"==="8 - Desempleo oculto")
println(DO.count)

// COMMAND ----------

// DBTITLE 1,Otro empleo no pleno
val OENP= data.where($"condicion_actividad"==="4 - Otro empleo no pleno")
println(OENP.count)

// COMMAND ----------

// DBTITLE 1,Subempleo por insuficiencia de ingresos
val SPII= data.where($"condicion_actividad"==="3 - Subempleo por insuficiencia de ingresos")
println(SPII.count)

// COMMAND ----------

data.select("nivel_de_instruccion").distinct().show

// COMMAND ----------

// DBTITLE 1,Tabla - Nivel de instrucción de Empleos Adecuados - Qué nos muestra ?-
import org.apache.spark.sql.functions._
EA.groupBy("nivel_de_instruccion").count().sort(desc("count")).show()

// COMMAND ----------

// DBTITLE 1,Tabla - Nivel de instrucción de Empleo no clasificado - Qué nos muestra ?-
import org.apache.spark.sql.functions._
ENC.groupBy("nivel_de_instruccion").count().sort(desc("count")).show()

// COMMAND ----------

// DBTITLE 1,Tabla - Nivel de instrucción por insuficiencia de tiempo de trabajo - Qué nos muestra ?-
import org.apache.spark.sql.functions._
SIT.groupBy("nivel_de_instruccion").count().sort(desc("count")).show()

// COMMAND ----------

// DBTITLE 1,Tabla - Nivel de instrucción por Desempleo oculto - Qué nos muestra ?-
import org.apache.spark.sql.functions._
DO.groupBy("nivel_de_instruccion").count().sort(desc("count")).show()

// COMMAND ----------

// DBTITLE 1,Tabla - Nivel de instrucción por insuficiencia de ingresos - Qué nos muestra ?-
import org.apache.spark.sql.functions._
SPII.groupBy("nivel_de_instruccion").count().sort(desc("count")).show()

// COMMAND ----------

// DBTITLE 1,Tabla - Nivel de instrucción por otro empleo no pleno - Qué nos muestra ?-
import org.apache.spark.sql.functions._
OENP.groupBy("nivel_de_instruccion").count().sort(desc("count")).show()

// COMMAND ----------

// DBTITLE 1,Ingreso laboral mínimo de los empleos adecuados
EA.select(min("ingreso_laboral")).show

// COMMAND ----------

// DBTITLE 1,Ingreso laboral máximo de los empleos adecuados
EA.select(max("ingreso_laboral")).show

// COMMAND ----------

// DBTITLE 1,Ingreso laboral máximo de los otros empleos no plenos
OENP.select(max("ingreso_laboral")).show

// COMMAND ----------

// DBTITLE 1,Promedio de los ingresos laborales de los otros empleos adecuados
EA.select(avg("ingreso_laboral")).show 

// COMMAND ----------

// DBTITLE 1,Porcentajes de sueldos
val sueldo1 = (EA.where($"ingreso_laboral">1000).count / EA.count.toDouble)*100

// COMMAND ----------

// DBTITLE 1,Porcentajes de sueldos
val sueldo1 = (EA.where($"ingreso_laboral"<1000).count / EA.count.toDouble)*100

// COMMAND ----------

// DBTITLE 1,Porcentajes de sueldos
val sueldo1 = (EA.where($"ingreso_laboral"=== null).count / EA.count.toDouble)*100

// COMMAND ----------

// DBTITLE 1,Porcentajes de sueldos
val sueldo1 = (EA.where($"ingreso_laboral">10000).count / EA.count.toDouble)*100

// COMMAND ----------

// DBTITLE 1,Porcentajes de sueldos
val sueldo1 = (EA.where($"ingreso_laboral">147000).count / EA.count.toDouble)*100

// COMMAND ----------

// DBTITLE 1,Porcentajes de sueldos
val sueldo2 = (DO.where($"ingreso_laboral"===null).count / EA.count.toDouble)*100

// COMMAND ----------


