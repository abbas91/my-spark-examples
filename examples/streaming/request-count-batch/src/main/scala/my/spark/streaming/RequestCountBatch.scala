package my.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._

object RequestCountBatch {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: RequestCountBatch <host> <port> <batchDuration> <filter>")
      System.exit(1)
    }  

    val hostname = args(0)
    val port = args(1).toInt
    val batchDuration = args(2).toInt
    val filter = args(3)
        
    println("\n" + "batchDuration: " + batchDuration + "\n")

    val ssc = new StreamingContext(new SparkConf(), Seconds(batchDuration))
    val logs = ssc.socketTextStream(hostname, port)
    val flogs = logs.filter(_.contains(filter))
    val batch = flogs.count().map(_.toInt)

    batch.foreachRDD((rdd, time) => {
      println("Time: " + time + ", Logs in Batch:  " + rdd.collect()(0))
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
