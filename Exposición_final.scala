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

import org.apache.spark.sql.functions._ //Importar librerias

// COMMAND ----------

val provinces = spark
  .read
  .option("InferSchema","true")
  .option("header","true")
  .option("delimiter","\t")
  .csv("/FileStore/tables/provincias.csv")

// COMMAND ----------

val Innerprovinces = data.join(provinces, data("provincia") === provinces("codigoprov"), "inner")

// COMMAND ----------

// DBTITLE 1,Cuantas personas que trabajan para las Fuerzas armadas tienen un  titulo de bachiller?
val fuerzas = data.select("grupo_ocupacion","nivel_de_instruccion")
fuerzas.where($"grupo_ocupacion" === "10 - Fuerzas Armadas" && $"nivel_de_instruccion" ==="06 - Secundaria").groupBy("grupo_ocupacion").count.show(false)

// COMMAND ----------

// DBTITLE 1,Cuantas personas dedicadas a la Agricultura, ganadería caza y silvicultura y pesca tienen ingresos mayores al salario básico?
val ramas = data.select("rama_actividad","ingreso_laboral")
ramas.where($"rama_actividad" === "01 - A. Agricultura, ganadería caza y silvicultura y pesca"&& $"ingreso_laboral" >= 400).groupBy("rama_actividad").count.show(false)

// COMMAND ----------

// DBTITLE 1,Cuantas personas dedicadas a la Agricultura, ganadería caza y silvicultura y pesca tienen ingresos menores al salario básico?
val ramas = data.select("rama_actividad","ingreso_laboral")
ramas.where($"rama_actividad" === "01 - A. Agricultura, ganadería caza y silvicultura y pesca"&& $"ingreso_laboral" < 400).groupBy("rama_actividad").count.show(false)

// COMMAND ----------

// DBTITLE 1,Cuantas personas dedicadas a la Agricultura, ganadería caza y silvicultura y pesca no tienen ingreso laboral fijo?
val ramas = data.select("rama_actividad","ingreso_laboral")
ramas.where($"rama_actividad" === "01 - A. Agricultura, ganadería caza y silvicultura y pesca"&& $"ingreso_laboral".isNull).groupBy("rama_actividad").count.show(false)

// COMMAND ----------

// DBTITLE 1,Cual es la provincia con más participantes dentro de la encuesta?
val encuesta = dataf.select("id","nombreprov")
encuesta.groupBy("nombreprov").count.orderBy(desc("count")).show(1)

// COMMAND ----------

// DBTITLE 1,Cual es el porcentaje de encuestados que pertenecen a la provincia de Loja?
val loja = dataf.select("nombreprov").where($"nombreprov"==="Loja")
val por_loja = (loja.count/dataf.count.toDouble)*100
print(loja.count)

// COMMAND ----------

// DBTITLE 1,Cual es el porcentaje de trabajadores menores a 18 años?
val trabajadores = dataf.select("edad").where($"edad"<18)
print(trabajadores.count)
val por_trabajdores = (trabajadores.count/dataf.count.toDouble)*100

// COMMAND ----------

// DBTITLE 1,Cual es el porcentaje de trabajadores menores a 18 años en Loja?
val loj = dataf.select("nombreprov","edad").where($"nombreprov"==="Loja")
val por_trabajos = (loj.where($"edad"<18).count/(dataf.count.toDouble))*100

// COMMAND ----------

// DBTITLE 1,Miembros de las fuerzas armadas según su nivel de instrucción?
val fuerzas2 = data.select("grupo_ocupacion","nivel_de_instruccion").where($"grupo_ocupacion" === "10 - Fuerzas Armadas")
fuerzas2.groupBy("nivel_de_instruccion").count.sort(desc("count")).show(false)
display(fuerzas2.groupBy("nivel_de_instruccion").pivot("grupo_ocupacion").count.orderBy("nivel_de_instruccion"))

// COMMAND ----------

val empleados = dataf.select("grupo_ocupacion","nombreprov").where($"grupo_ocupacion".isNull)
val empleados2 = empleados.where($"nombreprov" ==="Manabí" || $"nombreprov" ==="Santo Domingo de los Tsachilas" || $"nombreprov" ==="Santa Elena" || $"nombreprov" ==="El Oro" || $"nombreprov" ==="Esmeraldas" || $"nombreprov" ==="Guayas" || $"nombreprov" ==="Los Ríos")
display(empleados2.groupBy("nombreprov").pivot("grupo_ocupacion").count.orderBy("nombreprov"))

// COMMAND ----------

val prov = dataf.select("id","nombreprov").where($"nombreprov" === "Azuay" || $"nombreprov" === "Loja" || $"nombreprov" === "Pichincha" || $"nombreprov" === "Cañar" || $"nombreprov" === "Imbabura" || $"nombreprov" === "Bolivar" || $"nombreprov" === "Tungurahua" || $"nombreprov" === "Cotopaxi"  || $"nombreprov" === "Carchi" || $"nombreprov" === "Chimborazo")
display(prov.groupBy("nombreprov").count.orderBy(desc("count")))

// COMMAND ----------

val condicion = dataf.select("condicion_actividad","nombreprov").where($"nombreprov" ==="Loja" || $"nombreprov"==="Chimborazo")
val condicion2 = condicion.where($"condicion_actividad" ==="7 - Desempleo abierto" || $"condicion_actividad" ==="1 - Empleo Adecuado/Pleno" || $"condicion_actividad" ==="4 - Otro empleo no pleno" || $"condicion_actividad" ==="2 - Subempleo por insuficiencia de tiempo de trabajo" || $"condicion_actividad" ==="3 - Subempleo por insuficiencia de ingresos")
display(condicion2.groupBy("condicion_actividad").pivot("nombreprov").count.orderBy("condicion_actividad"))

// COMMAND ----------

val anios = dataf.select("anio","estado").where($"estado" === "1 - Casado(a)")
display(data.groupBy("anio").pivot("estado").count.orderBy("anio"))

// COMMAND ----------

val comp = data.select("condicion_actividad","genero").where($"condicion_actividad"==="7 - Desempleo abierto" || $"condicion_actividad" ==="3 - Subempleo por insuficiencia de ingresos" || $"condicion_actividad" ==="1 - Empleo Adecuado/Pleno" || $"condicion_actividad" ==="8 - Desempleo oculto")
display(comp.stat.crosstab("condicion_actividad","genero").orderBy("condicion_actividad_genero"))

// COMMAND ----------

// DBTITLE 1,Sentencias SQL
//Crear una vista temporal
dataf.createOrReplaceTempView("SQL_TABLE") 

// COMMAND ----------

//Emplear SQL y métodos spark/dataframe
spark.sql("""
    SELECT condicion_actividad as Condicion, sum(ingreso_laboral) 
    FROM SQL_TABLE
    GROUP BY condicion_actividad
""")
    .where ("condicion_actividad like '%e%'")
    .where("`sum(ingreso_laboral)` < 4500000")
    .show(false)

// COMMAND ----------


