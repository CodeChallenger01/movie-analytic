package sql

import read.SparkObject.spark
import sql.CreateTableByDF.{moviesTable, ratingsTable}

object SqlOperations extends App {

  moviesTable
  spark.sql(
    """
      |Select
      |MovieID,
      |substring(Title, 0, length(Title)-7) as MovieName,
      |substring(Title, length(Title) - 4, 4) as Year
      |From Movie
      |""".stripMargin
  ).createOrReplaceTempView("Movies")

  /* Oldest Movie*/
  spark.sql(
      """
        |Select
        |MovieID,
        |MovieName,
        |Year
        |From Movies
        |Where Year < "1945"
        |""".stripMargin
    )
    .show(false)

  /* Movies Each Year */
  spark.sql(
      """
        |Select
        |Year,
        |count(MovieID) as Count
        |From Movies
        |Group By Year
        |""".stripMargin
    )
    .show(false)

  ratingsTable.createOrReplaceTempView("Rating")
  spark.sql(
      """
        |Select
        |Ratings,
        |count(Ratings) as Total
        |From Rating
        |Group By Ratings
        |""".stripMargin
    )
    .show(false)

  spark.sql(
      """
        |Select
        |MovieID,
        |count(UserID) as Total
        |From Rating
        |Group By MovieID
        |Order By MovieID asc
        |""".stripMargin
    )
    .show(false)

  spark.sql(
      """
        |Select
        |MovieID,
        |sum(Ratings) as Total
        |From Rating
        |Group By MovieID
        |Order By MovieID
        |""".stripMargin
    )
    .show(20, false)

  spark.sql(
      """
        |Select
        |MovieID,
        |avg(Ratings) as Total
        |From Rating
        |Group By MovieID
        |Order By MovieID
        |""".stripMargin
    )
    .show(false)
}
