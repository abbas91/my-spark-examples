package my.spark.core

import org.apache.spark.SparkContext

object LineCount {
  def main(args: Array[String]) {
    if (args.length < 1 || args.length > 2) {
      System.err.println("Usage: LineCount <path> <filter>")
      System.exit(1)
    }
    var count:Long = 0
    val sc = new SparkContext()
    if (args.length == 1) {
      count = sc.textFile(args(0)).count()
    } else {
      count = sc.textFile(args(0)).filter(_.contains(args(1))).count()
    }
    println( "Line Count: " + count)
    sc.stop()
  }
}
