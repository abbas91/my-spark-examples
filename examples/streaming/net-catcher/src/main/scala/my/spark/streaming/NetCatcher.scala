package my.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._

object NetCatcher {
  def main(args: Array[String]) {
    if (args.length < 2 || args.length > 3) {
      println("Usage: NetCatcher <host> <port> [directory]")
      System.exit(1)
    }
    val host = args(0)
    val port = args(1).toInt
    val batchDuration = 2

    val sparkConf = new SparkConf().setAppName("NetCatcher")
    val ssc = new StreamingContext(sparkConf, Seconds(batchDuration))

    val lines = ssc.socketTextStream(host, port)
    val words = lines.flatMap(_.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey(_ + _)
    if (args.length == 3) {
      counts.saveAsTextFiles(args(2))
    } 
    counts.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
