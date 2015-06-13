package my.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkConfig {
  def main(args: Array[String]) {

    if (args.length > 1) {
      System.err.println("Usage: SparkConfig <appName>")
      System.exit(1)
    }

    if (args.length == 1) {
      val conf = new SparkConf().setAppName(args(0))
      val sc = new SparkContext(conf)
      println("Application Name: " + sc.appName)
      sc.stop()
    }
    
    else {
      val sc = new SparkContext()
      println("Application Name: " + sc.appName)
      sc.stop()
    }
  }
}
