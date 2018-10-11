package InnerJoins.SSQLDF

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import ScalaWriter.Writer.Write //to Write time


object DFIJ {

  def main(args: Array[String]): Unit = {
    //Setting log Levels...
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    //intializing Spark Session
    val ss = SparkSession.builder.master("local[2]")
      .appName("DF_DFIJ")
      .getOrCreate()
    //To use spark session implicit tools like the "$" operator...
    import ss.implicits._

    //Current time point log..
    val t0 = System.currentTimeMillis()

    //loading Users DataFrame...
    val InUserDF = ss.read.option("header","true").csv(args(0))
    InUserDF.take(10).foreach(println)

    //loading Purchases DataFrame...
    val InPurchseDF = ss.read.option("header","true").csv(args(1))
    InPurchseDF.take(10).foreach(println)

    val t1 = System.currentTimeMillis()

    val t2 = System.currentTimeMillis()

    // Joining the two DataFrames...
    val UserPurchase = InUserDF.join(InPurchseDF,"User_ID")
    UserPurchase.take(10).foreach(println)

    val t3 = System.currentTimeMillis()

    //-Select the Wanted Columns From the Joined DataFrame
    //--Concatenating First and Last Names as "Full_Name"...
    //--Multiplying unit amount and unit price as "Total_Price"...
    val UPDF1 = UserPurchase.select($"User_ID", concat($"First_Name", lit(" "), $"Last_name") as "Full_Name",
      $"Unit_Amount" * $"Unit_Price" as "Total_Price")
    UPDF1.take(10).foreach(println)

    //-Grouping by both User Id and the full name...
    val UPDF2 = UPDF1.groupBy($"User_ID",$"Full_Name")

    //-Aggrigating through the grouped values summing all of them as "Total_Purchase".
    val UserPurchase2 = UPDF2.agg(sum($"Total_Price") as "Total_Purchase")

    UserPurchase2.take(10).foreach(println)
    val t4 = System.currentTimeMillis()

//    UserPurchase2.coalesce(1).write.csv(args(2))

    val t5 = System.currentTimeMillis()
    val TRead = t1-t0
    val TPre = t2-t1
    val TJoin = t3-t2
    val TPost = t4-t3
    val TWrite = t5-t4
    println("Read, Pre-Processing, Join, Post-Processing, Write")
    val Line = TRead+","+TPre+","+TJoin+","+TPost+","+TWrite
    println(Line)
    Write(Line,"DF.IJ.csv")

    ss.stop()
  }
}
