package dataframe

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import read.ReadData.read_users
import read.SparkObject.spark

object ExtractUsersData {

  private val usersData = {
    read_users.map { users =>
      val user = users.split("::")
      Row(
        user.head.toInt,
        user(1),
        user(2).toInt,
        user(3).toInt,
        user(4).toInt
      )
    }
  }

  private val schema =
    StructType(
      Array(
        StructField("UserID", IntegerType, false),
        StructField("Gender", StringType, false),
        StructField("Age", IntegerType, false),
        StructField("Occupation", IntegerType, false),
        StructField("ZipCode", IntegerType, false)
      )
    )

  val usersDataframe =
    spark.createDataFrame(usersData, schema)

  val otherUsersDataframe =
    spark.read
      .format("csv")
      .option("delimiter", "::")
      .load("/home/knoldus/Desktop/KUP/DATA Bricks/movie-analytics/src/main/resources/users.dat")
      .toDF("UserID", "Gender", "Age", "Occupation", "Zipcode")
}
