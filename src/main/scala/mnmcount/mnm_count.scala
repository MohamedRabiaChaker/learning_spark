package main.scala.mnmcount

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Usage: MnMcount <dataset_path>}

object MnmCount {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("MnMcount").getOrCreate()

    if (args.length < 1) {
      print("Usage: MnMcount <mnm_file_dataset>")
      sys.exit(1)
    }

    val filename = args(0)
    val mnmDf = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(filename)

    val countedDf = mnmDf
      .select("State", "Color", "Count")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))
    countedDf.show(60)
    println()
    println("Selecting the state of California specifically")

    val countedCalDf = mnmDf
      .select("State", "Color", "Count")
      .where(col("State") === "CA")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))
    countedCalDf.show(10)
    spark.stop()
  }
}
