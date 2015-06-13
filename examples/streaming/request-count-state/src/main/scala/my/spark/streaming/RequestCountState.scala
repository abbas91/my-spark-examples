package my.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._

object RequestCountState {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: RequestCountState <host> <port> <batchDuration> <filter>")
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
      print("Time: " + time + ", Logs in Batch:  " + rdd.collect()(0))
    })

    ssc.checkpoint("checkpoints")
    val state = batch.map(count => ("count", count)).updateStateByKey(updateState)

    state.foreachRDD((rdd, time) => {
      println(", Total: " + rdd.collect()(0)._2)
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

  def updateState(newCounts: Seq[Int], state: Option[Int]): Option[Int] = {
    val newCount = newCounts.foldLeft(0)(_ + _)
    val previousCount = state.getOrElse(0)
    Some(newCount + previousCount)
  }
}
