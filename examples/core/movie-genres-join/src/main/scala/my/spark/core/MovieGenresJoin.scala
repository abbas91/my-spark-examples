package my.spark.core

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object MovieGenresJoin {

  def main(args: Array[String]) {

    val moviesSrc = "movielens/moviegenre"
    val genresSrc = "movielens/genre"

    val sc = new SparkContext()
    
    val movies = sc.textFile(moviesSrc).map(_.split("\t")).map(x => (x(1),x(0))).cache()
    println("\n*** (genreId, movieId)")
    movies.take(5).foreach(println)
    
    val genres = sc.textFile(genresSrc).map(_.split("\t")).map(x => (x(0),x(1))).cache()
    println("\n*** (genreId, genreName)")
    genres.take(5).foreach(println)
    
    val results = movies.join(genres).map{case (genreId,(movieId,genreName)) => (movieId,genreName)}.groupByKey()
    println("\n*** (movieId,genreNames)")
    for ((movieId,genreNames) <- results.take(8)) {println("(" + movieId + ",(" + genreNames.mkString(",") + "))")}

    println
    sc.stop()
  }
}
