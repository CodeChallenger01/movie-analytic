package sql

import read.SparkObject.spark
import sql.CreateTableByDF.ratingsTable

object CreateTable extends App {

  ratingsTable.createOrReplaceTempView("Rating")
  spark.sql("Drop database if exists SparkDB")
  spark.sql("Create database sparkDB")
  /* Save Movie Table */
  spark.sql(
    """
      |Select
      |MovieID,
      |substring(Title, 0, length(Title) - 7) as Title,
      |substring(Title, length(Title) - 4, 4) as Year,
      |Genres
      |From Movie
      |""".stripMargin
  ).write.mode("overwrite").saveAsTable("sparkDB.movie")

  /* Save Ratings Table */
  spark.sql(
    """
      |Select
      |UserID,
      |MovieID,
      |Ratings,
      |Timestamp
      |From Rating
      |""".stripMargin
  ).write.mode("overwrite").saveAsTable("sparkDB.rating")

  /* Saving User Table */
  spark.sql(
    """
      |Select
      |UserID,
      |Gender,
      |Age,
      |Occupation,
      |Zipcode
      |From User
      |""".stripMargin
  ).write.mode("overwrite").saveAsTable("sparkDB.user")
}
