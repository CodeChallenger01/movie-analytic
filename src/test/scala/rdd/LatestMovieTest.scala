package rdd

import org.scalatest.flatspec.AnyFlatSpec
import read.SparkObject.spark

class LatestMovieTest extends AnyFlatSpec {

  "latestMoviesWithFixYear" should "return the latest Movie Details" in {
    val actualOutput = LatestMovie.latestMoviesWithFixYear
    val unexpectedOutput = spark.sparkContext.parallelize(Seq("1223", "211212"))
    assert(actualOutput != unexpectedOutput)
  }

  "latestMoviesWithFixYear" should "return the equal count of row" in{
    val actualOutput = LatestMovie.latestMoviesWithFixYear.count()
    val expectedOutput = 160
    actualOutput == expectedOutput
  }

  "latestMoviesWithFixYear" should "not return the different length of row" in {
    val actualOutput = LatestMovie.latestMoviesWithFixYear.count()
    val expectedOutput = 240
    actualOutput != expectedOutput
  }

  "latestMovies" should "return the equal count of row" in {
    val actualOutput = LatestMovie.latestMovies.count()
    val expectedOutput = 160
    actualOutput == expectedOutput
  }

  "latestMovies" should "not return the different length of row" in {
    val actualOutput = LatestMovie.latestMovies.count()
    val expectedOutput = 240
    actualOutput != expectedOutput
  }
}
