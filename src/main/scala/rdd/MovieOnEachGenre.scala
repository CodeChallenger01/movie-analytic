package rdd

import read.ReadData.read_movies

object MovieOnEachGenre extends App {
  val movies =
    read_movies.flatMap { movies =>
        val movie = movies.split("::")
        val genres = movie(2).split('|').toList
        for {
          i <- genres
        } yield (i, 1)
      }
      .reduceByKey((x, y) => x + y)
      .sortByKey(ascending = true)

  movies.collect().foreach(println)

}
