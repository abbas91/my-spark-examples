package my.spark.core

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WordCount {
  def main(args: Array[String]) {

    if (args.length != 1) {
      println("Usage: WordCount <file or directory>")
      System.exit(1)
    }

    val sc = new SparkContext()
    val pairs = (sc.textFile(args(0))
      .flatMap(line => line.split("\\.|\\,|\\;|\\s"))
      .map(word => (word,1))
      .filter(pair => pair._1.length > 0)
      .reduceByKey((v1,v2) => v1+v2)
      .map(pair => (pair._2, pair._1))
      .sortByKey(false)
      .map(pair => (pair._2, pair._1)))
    pairs.take(5).foreach(pair => {
      println(pair._1 + ", " + pair._2)
    })
    sc.stop()
  }
}
