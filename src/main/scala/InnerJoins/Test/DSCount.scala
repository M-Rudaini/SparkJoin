package InnerJoins.Test

import org.apache.log4j._
import org.apache.spark.sql._


object DSCount {

  case class Products(Invoice_ID: Long, UID: Long, Product: String,
                      Unit_Amount: Int, Unit_Price: Float)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val ss = SparkSession.builder.master("local[2]").appName("SparkSQLDFjoin")
      .getOrCreate()
    import ss.implicits._

    val ProductsSchema = Encoders.product[Products].schema

    val InPurchseDS = ss.read.option("header","true").schema(ProductsSchema).csv(args(0)).as[Products]
    InPurchseDS.show(1)
    InPurchseDS.printSchema()
    val t2 = System.currentTimeMillis()

    println("DS Purchases Count: ",InPurchseDS.count())
    val t3 = System.currentTimeMillis()
    println("DS Count Time: " , t3-t2)

    ss.stop()
  }
}

