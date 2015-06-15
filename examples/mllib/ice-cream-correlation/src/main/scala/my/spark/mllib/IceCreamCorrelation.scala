package my.spark.mllib

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors

object IceCreamCorrelation {
  def main(args: Array[String]) {

    val sc = new SparkContext()

    val temps: RDD[Double] = sc.parallelize(Array(14.2, 16.4, 11.9, 15.2, 18.5, 22.1, 19.4, 25.1, 23.4, 18.1, 22.6, 17.2))
    val sales: RDD[Double] = sc.parallelize(Array(215, 325, 185, 332, 406, 522, 412, 614, 544, 421, 445, 408))

    println()
    println("Calculating correlation using two RDDs each containing an array of doubles:")
    println("---------------------------------------------------------------------------")
    println("temps: " + temps.collect().mkString(", "))
    println("sales: " + sales.collect().mkString(", "))
    println()
    println("Correlation using Pearson:  " + Statistics.corr(temps, sales, "pearson"))
    println("Correlation using Spearman: " + Statistics.corr(temps, sales, "spearman"))
    println()
    println("Calculating correlation matrix using one RDD containing an array of vectors:")
    println("----------------------------------------------------------------------------")

    val v1  = Vectors.dense(14.2, 215.0)
    val v2  = Vectors.dense(16.4, 325.0)
    val v3  = Vectors.dense(11.9, 185.0)
    val v4  = Vectors.dense(15.2, 332.0)
    val v5  = Vectors.dense(18.5, 406.0)
    val v6  = Vectors.dense(22.1, 522.0)
    val v7  = Vectors.dense(19.4, 412.0)
    val v8  = Vectors.dense(25.1, 614.0)
    val v9  = Vectors.dense(23.4, 544.0)
    val v10 = Vectors.dense(18.1, 421.0)
    val v11 = Vectors.dense(22.6, 445.0)
    val v12 = Vectors.dense(17.2, 408.0)
    val data: RDD[Vector] = sc.parallelize(Array(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12))

    println("Matrix using Pearson:")
    println(Statistics.corr(data, "pearson").toString())
    println()
    println("Matrix using Spearman:")
    println(Statistics.corr(data, "spearman").toString())
    println()
    sc.stop()
  }
}
