package InnerJoins.Test

import org.apache.log4j._
import org.apache.spark.sql._


object RDDCount {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val ss = SparkSession.builder.master("local[2]").appName("SparkSQLDFjoin")
      .getOrCreate()
    val prdd = ss.sparkContext.textFile(args(0)).filter(!_.contains("User_ID")).map(line => {
      val colArray = line.split(",")
      val UNo = colArray(3).toFloat
      val UPrice = colArray(4).toFloat
      val TPrice = UNo * UPrice
      colArray(1)+","+TPrice.toInt
    })
    prdd.take(10).foreach(println)
    val t0 = System.currentTimeMillis()

    println("RDD Purchases Count: " , prdd.count())
    val t1 = System.currentTimeMillis()
    println("RDD Count Time: " , t1-t0)

    ss.stop()
  }
}

