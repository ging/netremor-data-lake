package org.tfmupm

import org.apache.spark.sql.SparkSession
import io.delta.tables._


object ambulatoryReader {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Reading from tables")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val basepath = "D:/Archivos_uni/TFM/netremor-data-lake/src/main/scala/org/tfmupm/data"

//    val dfTableBronzeAmbulatory = spark.read.format("delta").load(s"$basepath/bronze_ambulatory")
//    val dfTableBronzeContinuous = spark.read.format("delta").load(s"$basepath/bronze_continuous")
//    val dfSubjectsTable = spark.read.format("delta").load(s"$basepath/SubjectsTable")
//    val dfSubjectRecords = spark.read.format("delta").load(s"$basepath/Subjects/96bdc1c7f436d03067f20092f73366bc5765cb958ff5e7a95635b5577815b62e/Records")
//    val dfSubjectAmbu = spark.read.format("delta").load(s"$basepath/Subjects/9bdc1c7f436d03067f200958ff5e7a95635b5577815b62e962f73366bc5765cb/Ambulatorio")
//    val dfSubjectTareasAmb = spark.read.format("delta").load(s"$basepath/Subjects/96bdc1c7f436d03067f20092f73366bc5765cb958ff5e7a95635b5577815b62e/Tasks/3hfe29c75h9e07hh08h2g3ghdcb0g8b19c8b193hfe2907hh08c8b1")
    val dfSubjectTareasCont = spark.read.format("delta").load(s"$basepath/Subjects/96bdc1c7f436d03067f20092f73366bc5765cb958ff5e7a95635b5577815b62e/Tasks/5i637igf3ad86i0f1hdcb1h9c2a44d846i9838d9b449647813bii19di3h4ih0d")


//    print("Tabla ambulatory bronze")
//    print("\n")
//    dfTableBronzeAmbulatory.show()
//    print("Tabla continuous bronze")
//    print("\n")
//    dfTableBronzeContinuous.show()
//
//    print("Tabla de los sujetos")
//    print("\n")
//    dfSubjectsTable.show()
//
//    print("Tabla de registros del sujeto 5c7a95635b5577815b629bdc200958ff5bee9621c7f436d03067ff73366bc576")
//    print("\n")
//    dfSubjectRecords.show()
//
//    print("Tabla de registros ambulatorios del sujeto 9bdc1c7f436d03067f200958ff5e7a95635b5577815b62e962f73366bc5765cb")
//    print("\n")
//    dfSubjectRecords.show()
//
//    print("Tabla tareas de registro ambulatorio del sujeto 9bdc1c7f436d03067f200958ff5e7a95635b5577815b62e962f73366bc5765cb")
//    print("\n")
//    dfSubjectAmbu.show()

    print("Tabla tareas de registro continuo del sujeto 96bdc1c7f436d03067f20092f73366bc5765cb958ff5e7a95635b5577815b62e")
    print("\n")
    dfSubjectTareasCont.show()



  }
}

