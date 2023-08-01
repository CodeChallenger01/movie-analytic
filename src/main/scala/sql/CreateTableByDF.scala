package sql

import org.apache.spark.sql.functions.{asc, avg, col, count, format_number, sum}
import read.SparkObject.spark

object CreateTableByDF {

  val moviesTable =
    spark.read
      .format("csv")
      .option("delimiter", "::")
      .load("/home/knoldus/Desktop/KUP/DATA Bricks/movie-analytics/src/main/resources/movies.dat")
      .toDF("MovieID", "Title", "Genres")
      .createOrReplaceTempView("Movie")

  val ratingsTable =
    spark.read
      .format("csv")
      .option("delimiter", "::")
      .load("/home/knoldus/Desktop/KUP/DATA Bricks/movie-analytics/src/main/resources/ratings.dat")
      .toDF("UserID", "MovieID", "Ratings", "Timestamp")

  val usersTable =
    spark.read
      .format("csv")
      .option("delimiter", "::")
      .load("/home/knoldus/Desktop/KUP/DATA Bricks/movie-analytics/src/main/resources/users.dat")
      .toDF("UserID", "Gender", "Age", "Occupation", "Zipcode")
      .createOrReplaceTempView("User")

  /* ALL SQL FUNCTIONALITY BY DATAFRAME*/
  private val readData = {
    spark.sql(
      """
        |Select
        |MovieID,
        |substring(Title, 0, length(Title)-7) as MovieName,
        |substring(Title, length(Title)-5, length(Title)-1) as Year,
        |Genres
        |FROM Movie
        |""".stripMargin)
  }

  val oldestReleasedMovies =
    readData.filter("Year < '(2000)'")

  val releasedMovieEachYear =
    readData.groupBy("Year")
      .agg(count("Year"))

  val numberOfMoviePerRating =
    ratingsTable
      .groupBy("ratings")
      .agg(
        count("ratings")
      )

  val userRatedEachMovie =
    ratingsTable.groupBy("MovieID")
      .agg(count("UserID")).orderBy(asc("MovieID"))

  val totalRatings =
    ratingsTable
      .groupBy("MovieID")
      .agg(
        sum(col("ratings")).as("Total")
      )
      .orderBy(asc("MovieID"))

  val averageRating =
    ratingsTable.groupBy("MovieID")
      .agg(
        format_number(avg("ratings"), 2).as("Average")
      ).orderBy(asc("MovieID"))
}
