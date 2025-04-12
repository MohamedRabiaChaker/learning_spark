package main.scala.mnmcount

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object avgAge {
    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder.appName(avgCount).getOrCreate()
  }
}
