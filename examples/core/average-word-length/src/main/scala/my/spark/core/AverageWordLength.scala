package my.spark.core

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object AverageWordLength {
  def main(args: Array[String]) {
    if (args.length != 1) {
      println("Usage: AverageWordLength <file or directory>")
      System.exit(1)
    }
    val sc = new SparkContext()
    val totalWords = sc.accumulator(0)
    val totalLetters = sc.accumulator(0.0)
    val words = sc.textFile(args(0)).flatMap(_.split(" "))
    words.foreach(word => {totalWords += 1; totalLetters += word.length})
    println( "Average word length: " + totalLetters.value/totalWords.value)
    sc.stop()
  }
}
