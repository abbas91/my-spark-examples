package my.spark.core

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.math.pow

object KMeansXY {

  def distanceSquared(p1:(Double,Double), p2:(Double,Double)) = { 
    pow(p1._1 - p2._1, 2) + pow(p1._2 - p2._2, 2)
  }

  def addPoints(p1:(Double,Double), p2:(Double,Double)) = {
    (p1._1 + p2._1, p1._2 + p2._2)
  }

  def closestPoint(p:(Double,Double), points:Array[(Double,Double)]): Int = {
    var index = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until points.length) {
      val dist = distanceSquared(p, points(i))
      if (dist < closest) {
        closest = dist
        index = i
      }
    }
    index
  }

  def main(args: Array[String]) {

    val coordinates = Array(
      (1,5),(3,4),(7,9),(2,2),(6,2),(12,3),(9,12),
      (4,6),(7,4),(9,11),(9,7),(10,10),(11,10),(2,3),
      (1,3),(10,1),(11,2),(10,3),(9,1),(9,3),(10,4),
      (2,9),(1,8),(2,5),(10,11),(2,7),(10,2))

    val K = 3
    val convergeDist = .1
    
    val sc = new SparkContext()

    val points = sc.parallelize(coordinates).map {case (x,y) => (x.toDouble, y.toDouble)}.cache()
    var kPoints = Array((1.0,1.0),(12.0,1.0),(12.0,12.0))
    
    println("\nStarting K points:")
    kPoints.foreach(println)
    println()

    var tempDist = Double.PositiveInfinity
    while (tempDist > convergeDist) {
      val closest = points.map(p => (closestPoint(p, kPoints), (p, 1)))
      val pointStats = closest.reduceByKey{case ((point1,n1),(point2,n2)) => (addPoints(point1,point2),n1+n2) }
      val newPoints = pointStats.map{case (i,(point,n)) => (i,(point._1/n,point._2/n))}.collectAsMap()
      tempDist = 0.0
      for (i <- 0 until K) tempDist += distanceSquared(kPoints(i),newPoints(i))
      println("Distance between iterations: " + tempDist)
      for (i <- 0 until K) kPoints(i) = newPoints(i)
    }

    println("\nFinal K points: ")
    kPoints.foreach(println)
    println
    sc.stop()
  }
}
