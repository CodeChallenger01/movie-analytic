package rdd

import read.ReadData.read_movies

object Genres {

  private val allGenres =
    read_movies.flatMap { movies =>
      val movie = movies.split("::")
      val genres = movie(2).split('|')
      genres.toList
    }

  val distinctGenres = allGenres.distinct()

}
