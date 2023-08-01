package rdd

import read.ReadData.read_movies

object MoviesStartingWithNumberAndAlphabet extends App {

  private val listOFCharacter = """~`@!#$%^&*(){}][':;?/><".,|\?"""
  private val allMovies =
    read_movies.map { movies =>
        val movie = movies.split("::")(1)
        (movie(0).toUpper, 1)
      }
      .filter(movie => !listOFCharacter.contains(movie._1))
      .reduceByKey((x, y) => x + y)
      .sortByKey(ascending = true)

  allMovies.collect().foreach(println)
}
