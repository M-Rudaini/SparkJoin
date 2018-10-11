package InnerJoins.Test

import org.apache.log4j._
import org.apache.spark.sql._


object DFCount {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val ss = SparkSession.builder.master("local[2]").appName("SparkSQLDFjoin")
      .getOrCreate()

    val InPurchseDF = ss.read.option("header","true").csv(args(0))
    InPurchseDF.show(1)
    InPurchseDF.printSchema()
    val t4 = System.currentTimeMillis()

    println("DF Purchases Count: ",InPurchseDF.count())
    val t5 = System.currentTimeMillis()
    println("DF Count Time: " , t5-t4)
    ss.stop()
  }
}

