package main.scala.mnmcount

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.types._

object avgAge {
  def calculateAverage() (implicit spark: SparkSession): DataFrame = {
    val schema = StructType(
      Seq(
        StructField("name", StringType, nullable = false),
        StructField("age", IntegerType, nullable = false)
      )
    )
    val data = Seq(
      ("Brooke", 20),
      ("Denny", 31),
      ("Jules", 30),
      ("TD", 35),
      ("Brooke", 25)
    )

    val df = spark.createDataFrame(data).toDF(schema.fieldNames: _*)
    val df_avg_age = df.groupBy("name").avg("age")

    return df_avg_age
  }

}
