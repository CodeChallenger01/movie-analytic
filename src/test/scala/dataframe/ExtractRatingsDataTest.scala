package dataframe

import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class ExtractRatingsDataTest extends AnyFlatSpec {

  /* DF through create Dataframe method */
  "ratingDataframe" should "return same number of column" in {
    val actualColumnCount = ExtractRatingsData.ratingDataframe.columns.length
    val expectedColumnCount = 4
    actualColumnCount shouldBe (expectedColumnCount)
  }

  "ratingDataframe" should "return the row if exists" in {
    val actualColumnCount = ExtractRatingsData.ratingDataframe.first()
    val expectedOutput = Row(1, 1193, 5, 978300760)
    actualColumnCount shouldBe expectedOutput
  }

  "ratingDataframe" should "not return different number of column" in {
    val actualColumnCount = ExtractRatingsData.ratingDataframe.columns.length
    val expectedColumnCount = 100
    assert(actualColumnCount != expectedColumnCount)
  }

  "ratingDataframe" should "match with type of Dataframe" in {
    val actualOutput = ExtractRatingsData.ratingDataframe
    assert(actualOutput.isInstanceOf[DataFrame])
  }

  "ratingDataframe" should "count of the record" in {
    val actualCount = ExtractRatingsData.ratingDataframe.count()
    val expectedCount = 1000209
    assert(actualCount == expectedCount)
  }

   /*Dataframe through spark read operation and csv format */
  "otherRatingDataframe" should "return same number of column" in {
    val actualColumnCount = ExtractRatingsData.otherRatingDataframe.columns.length
    val expectedColumnCount = 4
    actualColumnCount shouldBe (expectedColumnCount)
  }

  "otherRatingDataframe" should "not return different number of column" in {
    val actualColumnCount = ExtractRatingsData.otherRatingDataframe.columns.length
    val expectedColumnCount = 100
    assert(actualColumnCount != expectedColumnCount)
  }

  "otherRatingDataframe" should "match with type of Dataframe" in {
    val actualOutput = ExtractRatingsData.otherRatingDataframe
    assert(actualOutput.isInstanceOf[DataFrame])
  }

  "otherRatingDataframe" should "count of the record" in {
    val actualCount = ExtractRatingsData.otherRatingDataframe.count()
    val expectedCount = 1000209
    assert(actualCount == expectedCount)
  }

}
