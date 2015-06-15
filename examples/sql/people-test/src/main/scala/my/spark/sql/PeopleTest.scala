package my.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

case class Person(id: Int, fname: String, lname: String, email: String, country: String, occupation: String)

object PeopleTest {
  def main(args: Array[String]) {
    if (args.length != 1) {
      println("Usage: PeopleTest people.csv")
      System.exit(1)
    }

    val sc = new SparkContext()
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
    
    val people = sc.textFile(args(0)).map(_.split(",")).map(f => Person(f(0)toInt,f(1),f(2),f(3),f(4),f(5)))
    people.toDF().registerTempTable("people")
    
    println("select * from people limit 5")
    sqlContext.sql("select * from people limit 5").collect().foreach(println)
    
    val count = sqlContext.sql("select count(*) from people").collect().head.getLong(0)
    println(s"select count(*) from people = $count")

    val occupations = sqlContext.sql("select country, lname from people where occupation='Nurse'")    
    val countryOccupation = occupations.map(row => (row(0),row(1)))
    countryOccupation.take(5).foreach(println)

    sc.stop()
  }
}
