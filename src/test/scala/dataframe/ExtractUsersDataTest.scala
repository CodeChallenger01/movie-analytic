package dataframe

import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ExtractUsersDataTest extends AnyFlatSpec with Matchers {

  /* DF through create Dataframe method */
  "usersDataframe" should "return same number of column" in {
    val actualColumnCount = ExtractUsersData.usersDataframe.columns.length
    val expectedColumnCount = 5
    actualColumnCount shouldBe (expectedColumnCount)
  }

  "usersDataframe" should "return the row if exists" in {
    val actualColumnCount = ExtractUsersData.usersDataframe.first()
    val expectedOutput = Row(1, "F", 1, 10, "48067")
    actualColumnCount shouldBe expectedOutput
  }

  "usersDataframe" should "not return different number of column" in {
    val actualColumnCount = ExtractUsersData.usersDataframe.columns.length
    val expectedColumnCount = 100
    assert(actualColumnCount != expectedColumnCount)
  }

  "usersDataframe" should "match with type of Dataframe" in {
    val actualOutput = ExtractUsersData.usersDataframe
    assert(actualOutput.isInstanceOf[DataFrame])
  }

  "usersDataframe" should "count of the record" in {
    val actualCount = ExtractUsersData.usersDataframe.count()
    val expectedCount= 6040
    assert(actualCount == expectedCount)
  }

  /*Dataframe through spark read operation and csv format */
  "otherUsersDataframe" should "return same number of column" in {
    val actualColumnCount = ExtractUsersData.otherUsersDataframe.columns.length
    val expectedColumnCount = 5
    actualColumnCount shouldBe (expectedColumnCount)
  }

  "otherUsersDataframe" should "not return different number of column" in {
    val actualColumnCount = ExtractUsersData.otherUsersDataframe.columns.length
    val expectedColumnCount = 100
    assert(actualColumnCount != expectedColumnCount)
  }

  "otherUsersDataframe" should "match with type of Dataframe" in {
    val actualOutput = ExtractUsersData.otherUsersDataframe
    assert(actualOutput.isInstanceOf[DataFrame])
  }

  "otherUsersDataframe" should "count of the record" in {
    val actualCount = ExtractUsersData.otherUsersDataframe.count()
    val expectedCount = 6040
    assert(actualCount == expectedCount)
  }
}
