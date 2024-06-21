package org.tfmupm

import org.apache.spark.sql.SparkSession
import io.delta.tables._

import java.io.File
import java.nio.file.Paths

object SparkRead {
  def main(args: Array[String]): Unit = {
    val startTime = System.nanoTime()
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Reading from csv tables")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val baseDeltaDirectoryADL = "D:/Archivos_uni/TFM/TFMDataLake/src/main/scala/org/tfmupm/data/adl_sequences"
    val baseDeltaDirectoryHospital = "D:/Archivos_uni/TFM/TFMDataLake/src/main/scala/org/tfmupm/data/hospital_sequences"
    val baseDeltaDirectoryLab = "D:/Archivos_uni/TFM/TFMDataLake/src/main/scala/org/tfmupm/data/lab_sequences"

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

    val baseInputDirectoryADL = "D:/Archivos_uni/TFM/dataset/data/ADL_Sequences"
    val baseInputDirectoryHospital = "D:/Archivos_uni/TFM/dataset/data/hospital_sequences"
    val baseInputDirectoryLab = "D:/Archivos_uni/TFM/dataset/data/lab_sequences"
    processDirectory(new File(baseInputDirectoryADL), baseInputDirectoryADL, baseDeltaDirectoryADL)
    processDirectory(new File(baseInputDirectoryHospital), baseInputDirectoryHospital, baseDeltaDirectoryHospital)
    processDirectory(new File(baseInputDirectoryLab), baseInputDirectoryLab, baseDeltaDirectoryLab)

    val endTime = System.nanoTime()
    val duration = endTime - startTime
    val durationInSeconds = duration / 1e9
    println(s"El proceso duró $durationInSeconds segundos")
    spark.stop()
  }
}
