package rdd

import org.scalatest.flatspec.AnyFlatSpec
import read.SparkObject.spark

class GenresTest extends AnyFlatSpec {

  "it" should "return the all distinct Genres" in {
    val actualOutput = Genres.distinctGenres.count()
    val expectedOutput = spark.sparkContext.
      parallelize(
        Seq(
          "Fantasy",
          "Comedy",
          "Sci - Fi",
          "War",
          "Mystery",
          "Musical",
          "Documentary",
          "Animation",
          "Western",
          "Thriller",
          "Adventure",
          "Romance",
          "Horror",
          "Drama",
          "Crime",
          "Children's",
          "Action",
          "Film - Noir"
        )).count()
    assert(actualOutput == expectedOutput)
  }
}
