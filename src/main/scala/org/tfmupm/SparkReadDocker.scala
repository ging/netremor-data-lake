package org.tfmupm

import org.apache.spark.sql.SparkSession

import java.io.File
import java.nio.file.Paths

object SparkReadDocker {
  def main(args: Array[String]): Unit = {
    val startTime = System.nanoTime()
    val spark = SparkSession.builder()
      .master("spark://spark:7077")
      .appName("Transforming csv tables to Delta tables using Docker")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val originalPath = "/home/datalake/dataset" // Ruta donde se alojan los datos originales en formato CSV
    val dataPath = "/home/datalake/data/" // Ruta donde se guardarán las tablas en formato Delta

    val ADL_sequences_original = s"$originalPath/ADL_sequences"
    val hospital_sequences_original = s"$originalPath/hospital_sequences"
    val lab_sequences_original = s"$originalPath/lab_sequences"

    val ADL_sequences_delta = s"$dataPath/adl_sequences"
    val hospital_sequences_delta = s"$dataPath/hospital_sequences"
    val lab_sequences_delta = s"$dataPath/lab_sequences"


    // Se procesan los archivos CSV de las carpetas ADL_sequences, hospital_sequences y lab_sequences
    // y se guardan en formato Delta en las carpetas adl_sequences, hospital_sequences y lab_sequences
    // Para ello se itera por los directorios transormando los archivos CSV a tablas Delta

    def processDirectory(dir: File, baseInputPath: String, baseOutputPath: String): Unit = {
      val files = dir.listFiles()
      if (files != null) {
        files.foreach { file =>
          if (file.isDirectory) {
            processDirectory(file, baseInputPath, baseOutputPath)
          } else if (file.getName.endsWith(".csv")) {
            val relativePath = Paths.get(baseInputPath).relativize(Paths.get(file.getAbsolutePath)).toString
            val outputPath = Paths.get(baseOutputPath, relativePath).toString.replace(".csv", "")
            val deltaTablePath = new File(outputPath)
            if (!deltaTablePath.exists()) {
              println(s"Procesando el archivo: ${file.getName}")
              val df = spark.read.format("csv").option("header", "true").load(file.getAbsolutePath)
              df.write.format("delta").save(outputPath)
            } else {
              println(s"La tabla Delta para el archivo ${file.getName} ya existe, se omite su procesamiento.")
            }
          }
        }
      }
    }

    processDirectory(new File(ADL_sequences_original), ADL_sequences_original, ADL_sequences_delta)
    processDirectory(new File(hospital_sequences_original), hospital_sequences_original, hospital_sequences_delta)
    processDirectory(new File(lab_sequences_original), lab_sequences_original, lab_sequences_delta)

    val endTime = System.nanoTime()
    val duration = endTime - startTime
    val durationInSeconds = duration / 1e9
    println(s"El proceso duró $durationInSeconds segundos")
    spark.stop()
  }
}
