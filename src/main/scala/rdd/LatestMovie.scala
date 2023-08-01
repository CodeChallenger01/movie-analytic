package rdd

import read.ReadData.read_movies

import scala.annotation.tailrec

object LatestMovie {

  /* It is used when year should different */
  @tailrec
  def check(input: String): String = {
    val year = input.substring(input.indexOf("(") + 1, input.indexOf(")"))
    if (year.length == 4) year
    else {
      val newString = input.substring(input.indexOf(")") + 1)
      check(newString)
    }
  }

  val latestMovies =
    read_movies.map { movies =>
      val movie = movies.split("::")(1)
      val year = check(movie)
      val renameMovie = movie.substring(0, movie.indexOf(year) - 2)
      (renameMovie, year)
    }.filter(year => year._2 >= "2000")

  val latestMoviesWithFixYear =
    read_movies.filter { movies =>
      val movie = movies.split("::")(1)
      movie.contains("2000")
    }.map(_.split("::")(1))
}
