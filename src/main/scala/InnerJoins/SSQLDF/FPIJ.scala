package InnerJoins.SSQLDF

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import ScalaWriter.Writer.Write //to Write time


object FPIJ {

  def main(args: Array[String]): Unit = {
    //Setting log Levels...
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    //intializing Spark Session
    val ss = SparkSession.builder.master("local[2]")
      .appName("DF_FPIJ")
      .getOrCreate()
    //To use spark session implicit tools like the "$" operator...
    import ss.implicits._

    //Current time point log..
    val t0 = System.currentTimeMillis()

    //loading Users DataFrame...
    val InUserDF = ss.read.option("header","true").csv(args(0))

    //loading Purchases DataFrame...
    val InPurchseDF = ss.read.option("header","true").csv(args(1))

    val t1 = System.currentTimeMillis()

    //Selecting the Required Columns from the Users DataFrame...
    //Concatenate first and last names as new column "Full_Name"...
    val UserDF = InUserDF.select($"User_ID", concat($"First_Name", lit(" "), $"Last_Name")as "Full_Name")

    //Print the first 10 tuples...
    UserDF.show(10)

    //Selecting the Required Columns from the Users DataFrame...
    // to not conflict with the one in the Users DataFrame...
    val PDF1 = InPurchseDF.select($"User_ID", $"Unit_Amount" * $"Unit_Price" as "Total_Price")


    val PDF2 = PDF1.groupBy("User_ID")
    val PurchaseDF = PDF2.agg(sum("Total_Price") as "Total_Purchase")
    //Print the first 10 tuples...
    PurchaseDF.show(10)

    val t2 = System.currentTimeMillis()

    // Joining the two DataFrames...
    val UserPurchase = UserDF.join(PurchaseDF, "User_ID")
    UserPurchase.show(10)

    val t3 = System.currentTimeMillis()

    val t4 = System.currentTimeMillis()

//    UserPurchase.coalesce(1).write.csv(args(2))

    val t5 = System.currentTimeMillis()
    val TRead = t1-t0
    val TPre = t2-t1
    val TJoin = t3-t2
    val TPost = t4-t3
    val TWrite = t5-t4
    println("Read, Pre-Processing, Join, Post-Processing, Write")
    val Line = TRead+","+TPre+","+TJoin+","+TPost+","+TWrite
    println(Line)
    Write(Line,"DF.FPIJ.csv")

    ss.stop()
  }
}
