ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.11"
lazy val root = (project in file("."))
  .settings(
    name := "movie-analytics",
    coverageExcludedPackages :=
      """app.*;
        |sql.*;
        |rdd.TopViewedMovie;
        |rdd.MovieOnEachGenre;
        |rdd.MoviesStartingWithNumberAndAlphabet""".stripMargin,
    Compile / mainClass := Some("app.DriverApp")
  )
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.0",
  "org.apache.spark" %% "spark-sql" % "3.4.0",
  "org.scalatest" %% "scalatest" % "3.2.15"
)