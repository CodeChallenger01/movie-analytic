package dataframe

import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ExtractMoviesDataTest extends AnyFlatSpec with Matchers {

  /* DF through create Dataframe method */
  "movieDataFrame" should "return same number of column" in {
    val actualColumnCount = ExtractMoviesData.movieDataFrame.columns.length
    val expectedColumnCount = 4
    actualColumnCount shouldBe (expectedColumnCount)
  }

  "movieDataFrame" should "return the row if exists" in {
    val actualColumnCount = ExtractMoviesData.movieDataFrame.first()
    val expectedOutput = Row("1","Toy Story","1995","Animation,Children's,Comedy")
    actualColumnCount shouldBe  expectedOutput
  }

  "movieDataFrame" should "not return different number of column" in {
    val actualColumnCount = ExtractMoviesData.movieDataFrame.columns.length
    val expectedColumnCount = 100
    assert(actualColumnCount != expectedColumnCount)
  }

  "movieDataframe" should "match with type of Dataframe" in {
    val actualOutput = ExtractMoviesData.movieDataFrame
    assert(actualOutput.isInstanceOf[DataFrame])
  }

  "movieDataFrame" should "count of the record" in {
    val actualCount = ExtractMoviesData.movieDataFrame.count()
    val expectedCount = 3883
    assert(actualCount == expectedCount)
  }

  /* Dataframe through spark read operation and csv format */
  "otherMovieDataframe" should "return same number of column" in {
    val actualColumnCount = ExtractMoviesData.otherMovieDataframe.columns.length
    val expectedColumnCount = 4
    actualColumnCount shouldBe (expectedColumnCount)
  }

  "otherMovieDataframe" should "not return different number of column" in {
    val actualColumnCount = ExtractMoviesData.otherMovieDataframe.columns.length
    val expectedColumnCount = 100
    assert(actualColumnCount != expectedColumnCount)
  }

  "otherMovieDataframe" should "match with type of Dataframe" in {
    val actualOutput = ExtractMoviesData.otherMovieDataframe
    assert(actualOutput.isInstanceOf[DataFrame])
  }

  "otherMovieDataframe" should "count of the record" in {
    val actualCount = ExtractMoviesData.otherMovieDataframe.count()
    val expectedCount = 3883
    assert(actualCount == expectedCount)
  }
}
