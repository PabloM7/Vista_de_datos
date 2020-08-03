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

val dataf = Innerprovinces.drop("codigoprov").drop("provincia")

// COMMAND ----------

// DBTITLE 1,Cuantas personas que trabajan para las Fuerzas armadas tienen un  titulo de bachiller?
//Seleccionos la columnas con las que trabajaremos para crear un nuevo dataframe
val fuerzas = data.select("grupo_ocupacion","nivel_de_instruccion")
//Aplicamos un condicional where para seleccionar las Fuerzas Armadas y su nivel de Instruccion
//Agrupamos por grupo_ocupacion y realizamos un count para obtener el resultado.
fuerzas.where($"grupo_ocupacion" === "10 - Fuerzas Armadas" && $"nivel_de_instruccion" ==="06 - Secundaria").groupBy("grupo_ocupacion").count.show(false)

// COMMAND ----------

val fuerzas2 = data.select("grupo_ocupacion","nivel_de_instruccion").where($"grupo_ocupacion" === "10 - Fuerzas Armadas")//Seleccion del nuevo Dataframe
fuerzas2.groupBy("nivel_de_instruccion").count.sort(desc("count")).show(false) //Se agrupa por el nivel de instruccion y se realiza el conteno para ordenarlo de manera descendente
display(fuerzas2.groupBy("nivel_de_instruccion").pivot("grupo_ocupacion").count.orderBy("nivel_de_instruccion"))//Uso de la funcion display para crear las visualizaciones

// COMMAND ----------

// DBTITLE 1,¿Los trabajadores agrícolas y ganaderos tienen una buena remuneración por su trabajo?
//Ingreso laboral de trabajadores mayor al salario basico
//Seleccionos la columnas con las que trabajaremos para crear un nuevo dataframe
val ramas = data.select("rama_actividad","ingreso_laboral")
//Aplicamos un condicional where para seleccionar la agricultura y el nivel de ingreso
//Agrupamos por rama_actividad y realizamos un count para obtener el resultado.
ramas.where($"rama_actividad" === "01 - A. Agricultura, ganadería caza y silvicultura y pesca"&& $"ingreso_laboral" >= 400).groupBy("rama_actividad").count.show(false)

// COMMAND ----------

// DBTITLE 1,Ingreso laboral de trabajadores menor al salario básico
//Seleccionos la columnas con las que trabajaremos para crear un nuevo dataframe
val ramas = data.select("rama_actividad","ingreso_laboral")
//Aplicamos un condicional where para seleccionar la agricultura y el nivel de ingreso
//Agrupamos por rama_actividad y realizamos un count para obtener el resultado.
ramas.where($"rama_actividad" === "01 - A. Agricultura, ganadería caza y silvicultura y pesca"&& $"ingreso_laboral" < 400).groupBy("rama_actividad").count.show(false)

// COMMAND ----------

// DBTITLE 1,Trabajadores sin ingreso laboral fijo
//Seleccionos la columnas con las que trabajaremos para crear un nuevo dataframe
val ramas = data.select("rama_actividad","ingreso_laboral")
//Aplicamos un condicional where para seleccionar la agricultura y el nivel de ingreso, en este caso nulo
//Agrupamos por rama_actividad y realizamos un count para obtener el resultado.
ramas.where($"rama_actividad" === "01 - A. Agricultura, ganadería caza y silvicultura y pesca"&& $"ingreso_laboral".isNull).groupBy("rama_actividad").count.show(false)

// COMMAND ----------

//Se requieren dos variables porque es necesario colocar varios condicionales
val empleados = dataf.select("rama_actividad","anio","ingreso_laboral").where($"rama_actividad"==="01 - A. Agricultura, ganadería caza y silvicultura y pesca")
val empleados2 = empleados.where($"ingreso_laboral".isNull)//Condicion para mostrar los datos que no tienen sueldo fijo
display(empleados2.groupBy("anio").pivot("rama_actividad").count.orderBy("anio"))//Uso de display para crear la visualizacion 

// COMMAND ----------

// DBTITLE 1,¿Cuál es la provincia con más participantes dentro de la encuesta?
val encuesta = dataf.select("nombreprov")//Seleccion de las columnas para el nuevo Dataset
encuesta.groupBy("nombreprov").count.orderBy(desc("count")).show(1)// Mediante el show(1) solo se nos mostrara la fila más alta

// COMMAND ----------

////Seleccionos la columnas con las que trabajaremos para crear un nuevo dataframe
val encuesta = dataf.select("nombreprov").where($"nombreprov"==="Guayas" || $"nombreprov"==="Pichincha" || $"nombreprov"==="Tungurahua" || $"nombreprov"==="Manabí"|| $"nombreprov"==="Cotopaxi"|| $"nombreprov"==="El Oro" || $"nombreprov"==="Azuay")
//Agrupamos el dataframe con nombreprovincia, realizamos y ordenamos mediante  un count 
display(encuesta.groupBy("nombreprov").count.orderBy(desc("count"))) //En este caso se mostraran las 7 provincias que más aportan a la encuesta.

// COMMAND ----------

// DBTITLE 1,¿Cuantos trabajadores menores de 18 existen en la encuesta y a que se dedican?
val encuestados = dataf.select("edad").where($"edad"<18)// Seleccion del nuevo Dataset junto al condicional en la columna edad
val por_trabajos = (encuestados.count)//Count para mostrar el total de la consulta

// COMMAND ----------

val encuestados = dataf.select("edad").where($"edad"<18)// Seleccion del nuevo Dataset junto al condicional en la columna edad
display(encuestados.groupBy("edad").count)//Uso del display para crear una tabla dinamica.

// COMMAND ----------

val trabajadores = dataf.select("edad","condicion_actividad").where($"edad"<18)//Seleccionamos la columna para el dataframe y realizamos un condicional 
display(trabajadores.groupBy("condicion_actividad").pivot("edad").count)

// COMMAND ----------

// DBTITLE 1,Sentencias SQL
//Crear una vista temporal
dataf.createOrReplaceTempView("SQL_TABLE") 

// COMMAND ----------

//Emplear SQL y métodos spark/dataframe
spark.sql("""
    SELECT nombreprov as Provincia, count(*) 
    FROM SQL_TABLE
    GROUP BY nombreprov
""")
    .where ("nombreprov like '%Guayas%'")
    .show(1)

// COMMAND ----------

//Emplear SQL y métodos spark/dataframe
spark.sql("""
    SELECT rama_actividad, Count(*)
    FROM SQL_TABLE
    GROUP BY rama_actividad
""")
    .where ("rama_actividad like '%01 - A. Agricultura, ganadería caza y silvicultura y pesca%'")
    .show(false)

// COMMAND ----------

//Emplear SQL y métodos spark/dataframe
spark.sql("""
    SELECT edad, Count(*)
    FROM SQL_TABLE
    GROUP BY edad
""")
    .where("`edad` < 18")
    .show(false)

// COMMAND ----------


