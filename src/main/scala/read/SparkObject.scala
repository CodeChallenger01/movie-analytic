package read

import org.apache.spark.sql.SparkSession

object SparkObject {

  val spark = SparkSession.builder()
    .appName("movie_analyzer")
    .master("local[*]")
    .getOrCreate()

}
