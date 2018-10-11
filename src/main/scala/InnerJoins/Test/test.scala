package InnerJoins.Test

import org.apache.log4j._
import org.apache.spark.sql._


object test {

  case class Products(Invoice_ID: Long, UID: Long, Product: String,
                      Unit_Amount: Int, Unit_Price: Float)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val ss = SparkSession.builder.master("local[2]").appName("SparkSQLDFjoin")
      .getOrCreate()
    import ss.implicits._
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

    val ProductsSchema = Encoders.product[Products].schema

    val InPurchseDS = ss.read.option("header","true").schema(ProductsSchema).csv(args(0)).as[Products]
    InPurchseDS.show(1)
    InPurchseDS.printSchema()
    val t2 = System.currentTimeMillis()

    println("DS Purchases Count: ",InPurchseDS.count())
    val t3 = System.currentTimeMillis()
    println("DS Count Time: " , t3-t2)



    val InPurchseDF = ss.read.option("header","true").csv(args(0))
    InPurchseDF.show(1)
    InPurchseDF.printSchema()
    val t4 = System.currentTimeMillis()

    println("DF Purchases Count: ",InPurchseDS.count())
    val t5 = System.currentTimeMillis()
    println("DF Count Time: " , t5-t4)
    ss.stop()
  }
}

