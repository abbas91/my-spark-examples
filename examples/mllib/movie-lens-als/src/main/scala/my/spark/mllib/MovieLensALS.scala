package my.spark.mllib

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

object MovieLensALS {

  def main(args: Array[String]) {

    if (args.length != 5) {
      println("Usage: MovieLensALS [rank] [iterations] [regConst] [userId] [ratingsFile]")
      System.exit(1)
    }
    
    val rank = args(0).toInt
    val iterations = args(1).toInt
    val regConst = args(2).toDouble
    val userId = args(3).toInt
    val ratingsFile = args(4)

    val sc = new SparkContext()

    // Each record in the ratings RDD is a Rating (user, product, rating).
    val ratings = sc.textFile(ratingsFile).map(_.split("\t")).map(f => Rating(f(0).toInt,f(1).toInt,f(2).toDouble)).cache()
    // val ratings = sc.textFile(ratingsFile).map {line => val fields = line.split("\t"); Rating(fields(0).toInt,fields(1).toInt,fields(2).toDouble)}.cache()
    println(s"\nMovies:            " + ratings.map(_.product).distinct().count())
    println(s"Users:             " + ratings.map(_.user).distinct().count())
    println(s"Total Ratings:     " + ratings.count())

    // Array of RDDs.
    val splits = ratings.randomSplit(Array(0.8, 0.2))
    
    // The training RDD gets 80% of the ratings.
    val training = splits(0).cache()
    println(s"Training Ratings:  " + training.count())
    
    // The test RDD gets 20% of the ratings.
    val test = splits(1).cache()
    println(s"Test Ratings:      " + test.count())
    
    // All actual ratings for User 1
    val uActual = ratings.filter(_.user==userId).cache()

    // Don't need the ratings RDD anymore.
    ratings.unpersist(blocking = false)

    // Build the ALS model from the training RDD. 
    val model = new ALS().setRank(rank).setIterations(iterations).setLambda(regConst).setImplicitPrefs(false).run(training)
    println("\nSample of MatrixFactorizationModel records (productId, feature array):")
    model.productFeatures.takeSample(false,1).foreach {case (productId, array) => print(productId + " "); print(array.mkString(" ") + "\n")}

    // The predict method takes (userId, productId) and returns a Rating.
    val predictions: RDD[Rating] = model.predict(test.map(x => (x.user, x.product)))
    println("\nSample of predictions RDD records (productId, userId, rating):")
    predictions.takeSample(false,1).foreach(println)

    // Each record in this RDD consists of two ratings (predicted and actual) associated with a user and product.
    val predictionsAndRatings = predictions.map {x => ((x.user, x.product), x.rating)}.join(test.map(x => ((x.user, x.product), x.rating))).values
    println("\nSample of predictionsAndRatings RDD records (pRating, aRating):")
    predictionsAndRatings.takeSample(false,1).foreach(println)

    // The square root of the mean of the difference squared.
    val rmse = math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).mean())
    println("\nCalculation variables and RootMeanSquareError:")
    println("Rank:       " + rank)
    println("Iterations: " + iterations)
    println("RegConst:   " + regConst)
    println("RMSE:       " + rmse)
    
    // We know the actual ratings. Let's ignore those and see what the model predicts they should be.
    val uPred: RDD[Rating] = model.predict(uActual.map(x => (x.user, x.product)))
    println(f"\nSample of User $userId predicted ratings:")
    uPred.takeSample(false,1).foreach(x => println(x.user + "\t" + x.product + "\t" + x.rating))
    
    // Create uActualAndPred (product,(aRating, pRating))
    val uActualPair = uActual.map(x => (x.product,x.rating))
    val uPredPair = uPred.map(x => (x.product,x.rating))
    val uActualAndPred = uActualPair.join(uPredPair).sortByKey().map{case (productId,(aRating, pRating)) => (productId, aRating, pRating, aRating-pRating)}
    println(f"\nActual and predicted ratings for User $userId:")
    println("prodId\taRating\tpRating\tdiff")
    uActualAndPred.take(10).foreach {case (productId, aRating, pRating, diff) => println(f"$productId\t$aRating%1.1f\t$pRating%1.1f\t$diff%1.1f")}

    println()
    sc.stop()
  }
}
