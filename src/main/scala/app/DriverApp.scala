package app

import dataframe.ExtractMoviesData.{movieDataFrame, otherMovieDataframe}
import dataframe.ExtractRatingsData.{otherRatingDataframe, ratingDataframe}
import dataframe.ExtractUsersData.{otherUsersDataframe, usersDataframe}
import rdd.Genres.distinctGenres
import rdd.LatestMovie.{latestMovies, latestMoviesWithFixYear}
import sql.CreateTableByDF.{averageRating, numberOfMoviePerRating, oldestReleasedMovies, releasedMovieEachYear, totalRatings, userRatedEachMovie}

object DriverApp extends App {
  /* TODO Genres, MovieOnEachGenres, TopViewedMovie */
  /* --------RDD OPERATION -----------*/
  /* Top 10 Viewed Movies*/
  distinctGenres.collect().foreach(println)

  /* Latest Movies */
  latestMoviesWithFixYear.foreach(println)
  latestMovies.foreach(println)

  /* ---------DATAFRAME OPERATION--------- */

  /* Movie Dataframe */
  movieDataFrame.show(false)
  otherMovieDataframe.show(false)

  /* Ratings Dataframe */
  ratingDataframe.show(false)
  otherRatingDataframe.show(false)

  /* Users Dataframe */
  usersDataframe.show(false)
  otherUsersDataframe.show(false)

  /* --------------------SQL OPERATION------------------- */
  /* Oldest Released Movies */
  oldestReleasedMovies.show(false)

  /* Released Movie Each Year */
  releasedMovieEachYear.show(false)

  /* Number of Movie Per Ratings */
  numberOfMoviePerRating.show(false)

  /* Number of Movie Per Rating */
  numberOfMoviePerRating.show(false)

  /* User Rated Each Movie */
  userRatedEachMovie.show(false)

  /* Total Ratings */
  totalRatings.show(false)

  /* Average Ratings */
  averageRating.show(false)
}
