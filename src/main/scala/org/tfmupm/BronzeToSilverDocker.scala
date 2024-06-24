package org.tfmupm

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, lit}

import java.io.File
import java.nio.file.{Files, Paths}

object BronzeToSilverDocker {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("spark://spark:7077")
      .appName("Transforming from bronze to silver using Docker v2")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()


    // Se borran los datos de la carpeta data que no son la carpeta bronze. Sirve para automatizar el proceso de limpieza de datos.
    val dataPath = "/home/datalake/data/"

    // Se leen las tablas bronze que contiene todos los datos obtenidos en KafkaReaderWriter tanto de registros Ambulatorios como Continuos
    val dfTableBronzeAmbulatory = spark.read.format("delta").load(s"$dataPath/bronze_ambulatory_docker")
    val dfTableBronzeContinuous = spark.read.format("delta").load(s"$dataPath/bronze_continuous_docker")
    // Se guarda en la variable subjectTableA los datos de la tabla bronze_ambulatory y en la variable subjectTableB los datos de la tabla bronze_continuous
    // que se quieren guardar en la tabla silver.
    // Se combinan para obtener la tabla de todos los sujetos.
    val subjectTableA = dfTableBronzeAmbulatory.select("subject_id", "name", "diagnosis", "birth_year").dropDuplicates(Seq("subject_id", "name", "diagnosis"))
    val subjectTableC = dfTableBronzeContinuous.select("subject_id", "name", "diagnosis", "birth_year").dropDuplicates(Seq("subject_id", "name", "diagnosis"))
    val combinedSubjectTable = subjectTableA.union(subjectTableC)
    // Se eliminan los duplicados de la tabla combinada comprobando los campos subject_id, name y diagnosis
    val combinedSubjectTableNoDuplicates = combinedSubjectTable.dropDuplicates(Seq("subject_id", "name", "diagnosis"))
    // Se guarda la tabla silver en la carpeta SubjectsTable
    combinedSubjectTableNoDuplicates.write.format("delta").mode("overwrite").save(s"$dataPath/SubjectsTable")

    // Se muestra la tabla silver
    val subjectTableRead = spark.read.format("delta").load(s"$dataPath/SubjectsTable")
    subjectTableRead.show()

    // Se añade la columna record_type a las tablas bronze Ambulatory y Continuous
    val dfTableBronzeAmbulatoryWithType = dfTableBronzeAmbulatory.withColumn("record_type", lit("Ambulatorio"))
    val dfTableBronzeContinuousWithType = dfTableBronzeContinuous.withColumn("record_type", lit("Continuo"))

    // Agrupar por subject_id y crear una tabla para cada sujeto en cada DataFrame
    val subjectsAmbulatory = dfTableBronzeAmbulatoryWithType.select("subject_id").distinct().collect()
    val subjectsContinuous = dfTableBronzeContinuousWithType.select("subject_id").distinct().collect()

    // Por cada sujeto, se guardan sus registros en una tabla dependiendo de si son de tipo Ambulatory o Continuous
    subjectsAmbulatory.foreach { row =>
      val subjectId = row.getString(0)
      val subjectDFAmbulatory = dfTableBronzeAmbulatoryWithType.filter(col("subject_id") === subjectId)
      val subjectDFAmbulatoryNoDuplicates = subjectDFAmbulatory.dropDuplicates()

      // Guardar el DataFrame del sujeto en una tabla
      subjectDFAmbulatoryNoDuplicates.write.format("delta").mode("overwrite").save(s"$dataPath/Subjects/$subjectId/Ambulatorio")
    }

    subjectsContinuous.foreach { row =>
      val subjectId = row.getString(0)
      val subjectDFContinuous = dfTableBronzeContinuousWithType.filter(col("subject_id") === subjectId)
      val subjectDFContinuousNoDuplicates = subjectDFContinuous.dropDuplicates()

      // Guardar el DataFrame del sujeto en una tabla
      subjectDFContinuousNoDuplicates.write.format("delta").mode("overwrite").save(s"$dataPath/Subjects/$subjectId/Continuo")
    }


    val subjectsDir = new File(s"$dataPath/Subjects")
    val subjectIds = subjectsDir.listFiles().filter(_.isDirectory).map(_.getName)
    // Se guardan las tareas de cada registro
    subjectIds.foreach { subjectId =>
      val ambulatoryPath = s"$dataPath/Subjects/$subjectId/Ambulatorio"
      val continuousPath = s"$dataPath/Subjects/$subjectId/Continuo"

      if (new File(ambulatoryPath).exists()) {
        val dfSubjectAmbu = spark.read.format("delta").load(ambulatoryPath)

        // Obtener todos los record_id únicos
        val recordIdsAmbu = dfSubjectAmbu.select("record_id").distinct().collect()

        recordIdsAmbu.foreach { row =>
          val recordId = row.getString(0)

          // Filtrar las filas que corresponden a este record_id
          val recordDFAmbu = dfSubjectAmbu.filter(col("record_id") === recordId)

          // Hacer explode del campo recorded_tasks y seleccionar las columnas dentro de task
          val explodedDFAmbu = recordDFAmbu.select(col("record_id"), explode(col("recorded_tasks")).as("task"))
          val taskDetailsDFAmbu = explodedDFAmbu.select("task.*")

          // Seleccionar solo las columnas deseadas
          val selectedColumnsDFAmbu = taskDetailsDFAmbu.select("accelerometer_filename", "gyroscope_filename", "accelerometer_values", "gyroscope_values", "task_id", "task_name", "trial")

          // Guardar el DataFrame resultante en una tabla
          selectedColumnsDFAmbu.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(s"$dataPath/Subjects/$subjectId/Tasks/$recordId")
        }
      }

      if (new File(continuousPath).exists()) {
        val dfSubjectCont = spark.read.format("delta").load(continuousPath)

        // Obtener todos los record_id únicos
        val recordIdsCont = dfSubjectCont.select("record_id").distinct().collect()

        recordIdsCont.foreach { row =>
          val recordId = row.getString(0)

          // Filtrar las filas que corresponden a este record_id
          val recordDFCont = dfSubjectCont.filter(col("record_id") === recordId)

          // Hacer explode del campo recorded_tasks y seleccionar las columnas dentro de task
          val explodedDFCont = recordDFCont.select(col("record_id"), explode(col("recorded_tasks")).as("task"))
          val taskDetailsDFCont = explodedDFCont.select("task.*")

          // Seleccionar solo las columnas deseadas
          val selectedColumnsDFCont = taskDetailsDFCont.select("accelerometer_filename", "gyroscope_filename", "accelerometer_values", "gyroscope_values","task_id", "task_name", "starts_at", "ends_at")

          // Guardar el DataFrame resultante en una tabla
          selectedColumnsDFCont.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(s"$dataPath/Subjects/$subjectId/Tasks/$recordId")
        }
      }
    }
    // Como ya se han podido extraer las tareas de cada registro, se pueden unir los registros Ambulatory y Continuous en una sola tabla
    subjectIds.foreach { subjectId =>
      val ambulatoryPath = s"$dataPath/Subjects/$subjectId/Ambulatorio"
      val continuousPath = s"$dataPath/Subjects/$subjectId/Continuo"

      if (new File(ambulatoryPath).exists() && new File(continuousPath).exists()) {
        val dfSubjectAmbu = spark.read.format("delta").load(ambulatoryPath).drop("recorded_tasks")
        val dfSubjectCont = spark.read.format("delta").load(continuousPath).drop("recorded_tasks")

        // Unir los DataFrames
        val combinedDF = dfSubjectAmbu.union(dfSubjectCont)

        // Guardar el DataFrame resultante en una tabla
        combinedDF.write.format("delta").mode("overwrite").save(s"$dataPath/Subjects/$subjectId/Records")
      }
    }
  spark.stop()
  }
}



