package read

import read.SparkObject.spark

object ReadData {
  val read_ratings =
    spark.sparkContext
      .textFile("/home/knoldus/Desktop/KUP/DATA Bricks/movie-analytics/src/main/resources/ratings.dat", 4)

  val read_movies =
    spark.sparkContext
      .textFile("/home/knoldus/Desktop/KUP/DATA Bricks/movie-analytics/src/main/resources/movies.dat", 4)

  val read_users =
    spark.sparkContext
      .textFile("/home/knoldus/Desktop/KUP/DATA Bricks/movie-analytics/src/main/resources/users.dat", 4)
}
