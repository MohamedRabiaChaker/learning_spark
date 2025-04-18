package com.chaker.scripts.fifaAnalysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.FloatType

object FifaAnalysis {
  def analyseFifaData(path: String)(implicit spark: SparkSession): DataFrame = {

    val schema = StructType(
      Seq(
        StructField("country", StringType, nullable = false),
        StructField("conferderation", StringType, nullable = false),
        StructField("populationShare", FloatType, nullable = false),
        StructField("tvAudienceShare", FloatType, nullable = false),
        StructField("gdpWeightedShare", FloatType, nullable = false)
      )
    )

    val df =
      spark.read.schema(schema).option("header", true).format("csv").load(path)
    println("top 5 highest tv audience share countries")
    val topTvAudienceCountries = df
      .select("country", "populationShare", "tvAudienceShare")
      .orderBy(desc("tvAudienceShare"))

    println("sum of tv audience share by conferderation")
    val tvAudienceByConfederation = df
      .groupBy("conferderation")
      .agg(
        sum("tvAudienceShare").as("confederationTvAudienceShare"),
        sum("populationShare").as("confederationPopulationShare")
      )
      .orderBy(desc("confederationTvAudienceShare"))

    println("Turnup rate by confederation")
    val tvAudienceByPopulationShare = tvAudienceByConfederation
      .withColumn(
        "audiencePopulationPercentage",
        col("confederationTvAudienceShare") / col(
          "confederationPopulationShare"
        )
      )
      .orderBy(desc("audiencePopulationPercentage"))
    return tvAudienceByPopulationShare
  }
}
