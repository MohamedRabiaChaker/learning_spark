package com.chaker.jobs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.chaker.scripts.fifaAnalysis.FifaAnalysis

object SparkJob extends App {
  implicit val spark =
    SparkSession.builder.master("local[*]").appName("SparkJob").getOrCreate()

  println("Spark Job started:")
  val result = FifaAnalysis.analyseFifaData("/home/skrzydelka/Documents/learning_spark/src/main/scala/data/fifa_countries_audience.csv")
  result.show(10)
}
