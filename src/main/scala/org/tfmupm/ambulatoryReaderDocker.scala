package org.tfmupm

import org.apache.spark.sql.SparkSession
import io.delta.tables._


object ambulatoryReaderDocker {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("spark://spark:7077")
      .appName("Reading from tables using Docker")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val basepath = "/home/datalake/classes/org/tfmupm/data"

    val dfTableBronzeAmbulatory = spark.read.format("delta").load(s"$basepath/bronze_ambulatory")
    val dfTableBronzeContinuous = spark.read.format("delta").load(s"$basepath/bronze_continuous")
    val dfSubjectsTable = spark.read.format("delta").load(s"$basepath/SubjectsTable")
    //    val dfSubjectJaimeAmbu = spark.read.format("delta").load(s"$basepath/Subjects/9bdc1c7f436d03067f200958ff5e7a95635b5577815b62e962f73366bc5765cb/Ambulatorio")
    //    val dfSubjectJaimeCont = spark.read.format("delta").load(s"$basepath/Subjects/9bdc1c7f436d03067f200958ff5e7a95635b5577815b62e962f73366bc5765cb/Continuo")
    //    val dfSubjectIvanAmbu = spark.read.format("delta").load(s"$basepath/Subjects/c5765cb958ff5e7a95635b5577815b62e96bdc1c7f436d03067f20092f73366b/Ambulatorio")
    //    val dfSubjectIvanCont = spark.read.format("delta").load(s"$basepath/Subjects/c5765cb958ff5e7a95635b5577815b62e96bdc1c7f436d03067f20092f73366b/Continuo")
    val dfSubjectJaimeTareas1 = spark.read.format("delta").load(s"$basepath/Subjects/9bdc1c7f436d03067f200958ff5e7a95635b5577815b62e962f73366bc5765cb/Tasks/3c735c836702aa3385h872g9c4h526hfe29c75h9e07hh08ch2g3ghdcb0g8b193")
    val dfSubjectJaimeTareas2 = spark.read.format("delta").load(s"$basepath/Subjects/9bdc1c7f436d03067f200958ff5e7a95635b5577815b62e962f73366bc5765cb/Tasks/d947813bb4496i983h0d5i637igf3ad86i0f18ii19di3h4ihdcb1h9c2a44d846")
    val dfSubjectJaimeTareas3 = spark.read.format("delta").load(s"$basepath/Subjects/9bdc1c7f436d03067f200958ff5e7a95635b5577815b62e962f73366bc5765cb/Tasks/hfe29c75h9e07hh08ch2g3ghdcb0g8b1933c735c836702aa3385h872g9c4h526")

    val dfSubjectJaimeRecords = spark.read.format("delta").load(s"$basepath/Subjects/9bdc1c7f436d03067f200958ff5e7a95635b5577815b62e962f73366bc5765cb/Records")

    print("Tabla ambulatory bronze")
    dfTableBronzeAmbulatory.show()
    print("Tabla continuous bronze")
    dfTableBronzeContinuous.show()

    print("Tabla sujetos")
    //dfSubjectsTable.show()
    print("Tabla de registros del sujeto Jaime")
    //dfSubjectJaimeRecords.show()

    print("Tabla tareas 1del sujeto Jaime")
    //dfSubjectJaimeTareas1.show()
    print("Tabla tareas 2 del sujeto Jaime")
    //dfSubjectJaimeTareas2.show()
    //print("Tabla tareas 3 del sujeto Jaime")
    //dfSubjectJaimeTareas3.show()



  }
}

