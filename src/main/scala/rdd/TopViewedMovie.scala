package rdd

import read.ReadData._

object TopViewedMovie extends App {

  private val top_viewed_movies = {
    read_ratings.map { row =>
      val line = row.split("::").toList
      val movieID = line(1)
      (movieID, 1)
    }.reduceByKey((x, y) => x + y).map(x => (x._2, x._1))
  }.sortByKey(ascending = false).take(10).toList

  private val getMoviesName =
    read_movies.filter { movies =>
      val movie = movies.split("::").toList
      val id = movie.head
      val value = top_viewed_movies.filter(x => x._2 == id)
      value.nonEmpty
    }

  val result = getMoviesName.flatMap { rows =>
    val row = rows.split("::")
    top_viewed_movies.map { movie =>
      movie._2 match {
        case exit if exit == row.head => (movie._1, row(0), row(1))
        case _ => None
      }
    }
  }.filter(movie => movie != None)

  result.foreach(println)
}

