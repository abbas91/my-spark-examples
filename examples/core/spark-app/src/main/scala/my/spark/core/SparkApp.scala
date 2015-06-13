package my.spark.core

import org.apache.spark.SparkContext

object SparkApp {
  def main(args: Array[String]) {
    if (args.length != 0) {
      println("Usage: SparkApp")
      System.exit(1)
    }
    val sc = new SparkContext()
    println( "sc.appName = " + sc.appName)
    sc.stop()
  }
}
