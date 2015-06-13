package my.spark.graphx

import org.apache.spark.SparkContext
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._

object PageRankGX {
  def main(args: Array[String]) {
    if (args.length != 2) {
      println("Usage: PageRankGX edgeFile numIter")
      System.exit(1)
    }

    val sc = new SparkContext()

    val graph = GraphLoader.edgeListFile(sc, args(0)).cache()

    println("Number of vertices: " + graph.vertices.count)
    println("Number of edges:    " + graph.edges.count)
    
    val ranks = PageRank.run(graph, args(1).toInt).vertices.cache()
    ranks.collect().foreach(println)

    sc.stop()
  }
}
