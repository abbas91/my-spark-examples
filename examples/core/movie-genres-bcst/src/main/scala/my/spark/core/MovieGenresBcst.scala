package my.spark.core

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.io.Source

object MovieGenresBcst {

  def main(args: Array[String]) {

    val moviesSrc = "movielens/moviegenre"
    val genresSrc = "/tmp/genre.txt"

    val sc = new SparkContext()
    
    val movies = sc.textFile(moviesSrc).map(_.split("\t")).map(x => (x(1),x(0))).cache()
    println("\n*** (genreId, movieId)")
    movies.take(5).foreach(println)
    
    val genres = Source.fromFile(genresSrc).getLines.map(_.split("\t")).map(x => (x(0),x(1))).toMap
    println("\n*** (genreId, genreName)")
    genres.take(5).foreach(println)
    val genresbc = sc.broadcast(genres)
    
    val results = movies.map{case (genreId,movieId) => (movieId, genresbc.value(genreId))}.groupByKey()
    println("\n*** (movieId,genreNames)")
    for ((movieId,genreNames) <- results.take(8)) {println("(" + movieId + ",(" + genreNames.mkString(",") + "))")}

    println
    sc.stop()
  }
}
