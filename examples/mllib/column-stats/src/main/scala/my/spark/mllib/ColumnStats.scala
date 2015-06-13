package my.spark.mllib

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.mllib.stat.Statistics

object ColumnStats {
  def main(args: Array[String]) {

    val sc = new SparkContext()

    val v1 = Vectors.dense(1.1, 2.8, 3.6, 4.3)
    val v2 = Vectors.dense(1.5, 2.2, 3.1, 4.4)
    val v3 = Vectors.dense(1.2, 2.8, 3.9, 4.9)
    val v4 = Vectors.dense(1.6, 0.0, 3.6, 4.2)

    val observations = sc.parallelize(Array(v1, v2, v3, v4)).cache()
    val rows = observations.collect()
    val summary = Statistics.colStats(observations)

    println()
    for(i <- 0 until rows(0).size){print(f"\tC$i")}
    println("\n")

    println("\tOBSERVATIONS")
    for(i <- 0 until rows.length){
      print(f"R$i:")
      var items = rows(i).toArray
      for(j <- 0 until items.length){var item = items(j); print(f"\t$item")}
      println()
    }

    println("\n\tSUMMARY STATISTICS")

    print("Max:")
    val maxArray = summary.max.toArray
    for(i <- 0 until maxArray.length){var v = maxArray(i); print(f"\t$v%1.1f")}
    println()

    print("Min:")
    val minArray = summary.min.toArray
    for(i <- 0 until minArray.length){var v = minArray(i); print(f"\t$v%1.1f")}
    println()

    print("Mean:")
    val meanArray = summary.mean.toArray
    for(i <- 0 until meanArray.length){var v = meanArray(i); print(f"\t$v%1.1f")}
    println()

    print("NrmL1:")
    val nrmL1Array = summary.normL1.toArray
    for(i <- 0 until nrmL1Array.length){var v = nrmL1Array(i); print(f"\t$v%1.1f")}
    println()

    print("NrmL2:")
    val nrmL2Array = summary.normL2.toArray
    for(i <- 0 until nrmL2Array.length){var v = nrmL2Array(i); print(f"\t$v%1.1f")}
    println()

    print("Non-0:")
    val non0Array = summary.numNonzeros.toArray
    for(i <- 0 until non0Array.length){var v = non0Array(i); print(f"\t$v%1.1f")}
    println()

    print("Var:")
    val varArray = summary.variance.toArray
    for(i <- 0 until varArray.length){var v = varArray(i); print(f"\t$v%1.1f")}
    println("\n")

    sc.stop()
  }
}
