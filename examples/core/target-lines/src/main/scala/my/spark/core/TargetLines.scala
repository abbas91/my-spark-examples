package my.spark.core

import org.apache.spark.SparkContext
import org.apache.commons.lang.StringUtils

object TargetLines {

  def main(args: Array[String]) {

    if (args.length != 2) {
      System.err.println("Usage: TargetLines <path> <list>")
      System.exit(1)
    }

    val sc = new SparkContext()
    val listbc = sc.broadcast(args(1).split(" "))
    val targetLines = sc.textFile(args(0)).filter(line => listbc.value.count(StringUtils.containsIgnoreCase(line,_)) > 0)
    targetLines.foreach(println)
    sc.stop()
  }
}
