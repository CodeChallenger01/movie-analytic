package dataframe

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import rdd.LatestMovie.check
import read.ReadData.read_movies
import read.SparkObject.spark

object ExtractMoviesData {

  /*private def check(input: String): String = {
    val year = input.substring(input.indexOf("(") + 1, input.indexOf(")"))
    if (year.length == 4) year
    else {
      val newString = input.substring(input.indexOf(")") + 1)
      check(newString)
    }
  }*/

  private val movieData = {
    read_movies.map { movies =>
      val movieDetails = movies.split("::")
      val year = check(movieDetails(1))
      val movie = movieDetails(1).substring(0, movieDetails(1).indexOf(year) - 2)
      Row(
        movieDetails.head,
        movie,
        year,
        movieDetails(2).replace('|', ','))
    }
  }

  private val schema =
    StructType(
      Array(
        StructField("MovieID", StringType, false),
        StructField("Title", StringType, true),
        StructField("Year", StringType, false),
        StructField("Genres", StringType, true)
      )
    )

  val movieDataFrame =
    spark.createDataFrame(movieData, schema = schema)


  private val dataframe =
    spark.read
      .format("csv")
      .option("delimiter", "::")
      .load("/home/knoldus/Desktop/KUP/DATA Bricks/movie-analytics/src/main/resources/movies.dat")
      .toDF("MovieID", "Title", "Genres")


  val otherMovieDataframe =
    dataframe
      .withColumn("Movie", regexp_extract(col("Title"), "[^(0-9]+", 0)) /*"[A-Za-z\\s]+"*/
      .withColumn("Year", regexp_extract(col("Title"), exp = "(\\d{4})", 0))
      .select(
        "MovieID",
        "Movie",
        "Year",
        "Genres"
      )
}
