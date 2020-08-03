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

// DBTITLE 1,¿Cuantas personas que trabajan para las Fuerzas armadas tienen un  titulo de bachiller?
//Seleccionos la columnas con las que trabajaremos para crear un nuevo dataframe
val fuerzas = data.select("grupo_ocupacion","nivel_de_instruccion")
//Aplicamos un condicional where para seleccionar las Fuerzas Armadas y su nivel de Instruccion
//Agrupamos por grupo_ocupacion y realizamos un count para obtener el resultado.
fuerzas.where($"grupo_ocupacion" === "10 - Fuerzas Armadas" && $"nivel_de_instruccion" ==="06 - Secundaria").groupBy("grupo_ocupacion").count.show(false)

// COMMAND ----------

// DBTITLE 1,Cuantas personas dedicadas a la Agricultura, ganadería caza y silvicultura y pesca tienen ingresos mayores al salario básico?
//Seleccionos la columnas con las que trabajaremos para crear un nuevo dataframe
val ramas = data.select("rama_actividad","ingreso_laboral")
//Aplicamos un condicional where para seleccionar la agricultura y el nivel de ingreso
//Agrupamos por rama_actividad y realizamos un count para obtener el resultado.
ramas.where($"rama_actividad" === "01 - A. Agricultura, ganadería caza y silvicultura y pesca"&& $"ingreso_laboral" >= 400).groupBy("rama_actividad").count.show(false)

// COMMAND ----------

// DBTITLE 1,Cuantas personas dedicadas a la Agricultura, ganadería caza y silvicultura y pesca tienen ingresos menores al salario básico?
val ramas = data.select("rama_actividad","ingreso_laboral")
ramas.where($"rama_actividad" === "01 - A. Agricultura, ganadería caza y silvicultura y pesca"&& $"ingreso_laboral" < 400).groupBy("rama_actividad").count.show(false)

// COMMAND ----------

// DBTITLE 1,Cuantas personas dedicadas a la Agricultura, ganadería caza y silvicultura y pesca no tienen ingreso laboral fijo?
//Seleccionos la columnas con las que trabajaremos para crear un nuevo dataframe
val ramas = data.select("rama_actividad","ingreso_laboral")
//Aplicamos un condicional where para seleccionar la agricultura y el nivel de ingreso, en este caso nulo
//Agrupamos por rama_actividad y realizamos un count para obtener el resultado.
ramas.where($"rama_actividad" === "01 - A. Agricultura, ganadería caza y silvicultura y pesca"&& $"ingreso_laboral".isNull).groupBy("rama_actividad").count.show(false)

// COMMAND ----------

val encuesta = dataf.select("nombreprov")
encuesta.groupBy("nombreprov").count.orderBy(desc("count")).show(1)

// COMMAND ----------

// DBTITLE 1,Cual es la provincia con más participantes dentro de la encuesta?
////Seleccionos la columnas con las que trabajaremos para crear un nuevo dataframe
val encuesta = dataf.select("nombreprov").where($"nombreprov"==="Guayas" || $"nombreprov"==="Pichincha" || $"nombreprov"==="Tungurahua" || $"nombreprov"==="Manabí"|| $"nombreprov"==="Cotopaxi"|| $"nombreprov"==="El Oro" || $"nombreprov"==="Azuay")
//Agrupamos el dataframe con nombreprovincia y realizamos un count junto al numero de id
display(encuesta.groupBy("nombreprov").count.orderBy(desc("count"))) // Con el show(1) mostraremos el resultado más alto al estar en orden descendente.


// COMMAND ----------

// DBTITLE 1,Cuantos trabajadores menores de 18 existen en la encuesta?
val encuestados = dataf.select("edad").where($"edad"<18)
val por_trabajos = (encuestados.count)
display(encuestados.groupBy("edad").count)

// COMMAND ----------

val trabajadores = dataf.select("edad","condicion_actividad").where($"edad"<18)//Seleccionamos la columna para el dataframe y realizamos un condicional 
val trabajadores2 = trabajadores.where($"condicion_actividad"==="5 - Empleo no remunerado")
display(trabajadores.groupBy("condicion_actividad").pivot("edad").count)
//val por_trabajdores = (trabajadores.count/dataf.count.toDouble)*100 // Realizamos un count de la columna y los dividimos para un count completo del dataset, tambien se debe trasnformar el resultado en double ya que trabajamos con porcentajes.

// COMMAND ----------

// DBTITLE 1,Miembros de las fuerzas armadas según su nivel de instrucción?
val fuerzas2 = data.select("grupo_ocupacion","nivel_de_instruccion").where($"grupo_ocupacion" === "10 - Fuerzas Armadas")
fuerzas2.groupBy("nivel_de_instruccion").count.sort(desc("count")).show(false)
display(fuerzas2.groupBy("nivel_de_instruccion").pivot("grupo_ocupacion").count.orderBy("nivel_de_instruccion"))

// COMMAND ----------

dataf.select("rama_actividad").distinct.show(false)

// COMMAND ----------

val empleados = dataf.select("rama_actividad","anio","ingreso_laboral").where($"rama_actividad"==="01 - A. Agricultura, ganadería caza y silvicultura y pesca")
val empleados2 = empleados.where($"ingreso_laboral".isNull)
display(empleados2.groupBy("anio").pivot("rama_actividad").count.orderBy("anio"))

// COMMAND ----------

val empleados = dataf.select("rama_actividad","anio","ingreso_laboral").where($"rama_actividad"==="01 - A. Agricultura, ganadería caza y silvicultura y pesca")
val empleados2 = empleados.where($"ingreso_laboral">=400)
display(empleados2.groupBy("anio").pivot("rama_actividad").count.orderBy("anio"))

// COMMAND ----------

val empleados = dataf.select("rama_actividad","anio","ingreso_laboral").where($"rama_actividad"==="01 - A. Agricultura, ganadería caza y silvicultura y pesca")
val empleados2 = empleados.where($"ingreso_laboral"<400)
display(empleados2.groupBy("anio").pivot("rama_actividad").count.orderBy("anio"))

// COMMAND ----------

val prov = dataf.select("id","nombreprov").where($"nombreprov" ==="Manabí" || $"nombreprov" ==="Santo Domingo de los Tsachilas" || $"nombreprov" ==="Santa Elena" || $"nombreprov" ==="El Oro" || $"nombreprov" ==="Esmeraldas" || $"nombreprov" ==="Guayas" || $"nombreprov" ==="Los Ríos")
display(prov.groupBy("nombreprov").count.orderBy(desc("count")))

// COMMAND ----------

val condicion = dataf.select("condicion_actividad","nombreprov").where($"nombreprov" ==="Guayas" || $"nombreprov"==="Manabí")
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


