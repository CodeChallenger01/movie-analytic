package dataframe

import org.apache.spark.sql._
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import read.ReadData.read_ratings
import read.SparkObject.spark

object ExtractRatingsData {

  private val ratingsData =
    read_ratings.map { ratings =>
      val rating = ratings.split("::")
      Row(
        rating.head.toInt,
        rating(1).toInt,
        rating(2).toInt,
        rating(3).toLong
      )
    }

  private val schema =
    StructType(
      Array(
        StructField("UserID", IntegerType, true),
        StructField("MovieID", IntegerType, true),
        StructField("Ratings", IntegerType, true),
        StructField("TimeStamp", LongType, true),
      )
    )

  val ratingDataframe =
    spark.createDataFrame(ratingsData, schema)


  val otherRatingDataframe =
    spark.read
      .format("csv")
      .option("delimiter", "::")
      .csv("/home/knoldus/Desktop/KUP/DATA Bricks/movie-analytics/src/main/resources/ratings.dat")
      .toDF("UserID", "MovieID", "Ratings", "Timestamp")

}