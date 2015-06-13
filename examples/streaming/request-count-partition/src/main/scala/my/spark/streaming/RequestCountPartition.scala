package my.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._

object RequestCountPartition {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: RequestCountPartition <host> <port> <batchDuration> <filter>")
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
      println("*** RDD Id = " + rdd.id + ", Partition Count = " + rdd.partitions.size)
      rdd.foreachPartition { partition =>
        val iters = partition.duplicate
        println("Partition Item Count = " + iters._1.length)
        iters._2.foreach { item =>
          println("Time: " + time + ", Logs in Partition:  " + item)
        }
      }
      println()
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
