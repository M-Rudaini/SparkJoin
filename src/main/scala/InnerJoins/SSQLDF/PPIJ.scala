package InnerJoins.SSQLDF
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
//  Initializing the DF Post-Processed Inner-Join.
object PPIJ {
  //  Defining the main method.
  def main(args: Array[String]): Unit = {
    // Setting the logger Levels.
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    // Initializing Spark Session
    val ss = SparkSession
      .builder  
      .appName("DF_PPIJ")
      .getOrCreate()
    // To use spark session implicit tools like the "$" operator...
    import ss.implicits._
    // loading Users DataFrame...
    // Considering the header as Column Names that will be used later...
    val InUserDF = ss.read.option("header","true")
     .csv(args(0))  // Source Connector.
     .persist()   //  Caching the InUserDF contents for further use...
    InUserDF.take(10).foreach(println)   // Printing First 10 Items.
    // loading Purchases DataFrame...
    // Considering the header as Column Names that will be used later...
    val InPurchseDF = ss.read.option("header","true")
     .csv(args(1)) // Source Connector.
     .persist()   //  Caching the InPurchseDF contents for further use...
    InPurchseDF.take(10).foreach(println)  // Printing First 10 Items.
    //  Joining the two DataFrames...
    val UserPurchase = InUserDF.join(InPurchseDF,"User_ID")
    // -Select the Wanted Columns from the Joined DataFrame
    // --Concatenating First and Last Names as "Full_Name"...
    // --Multiplying unit amount and unit price as "Total_Price"...
    .select($"User_ID", concat($"First_Name", lit(" "), $"Last_name") as "Full_Name", $"Unit_Amount" * $"Unit_Price" as "Total_Price")
    // -Grouping by both User ID and the Full Name...
    .groupBy($"User_ID",$"Full_Name")
    // -Aggregating and summing all the grouped values as "Total_Purchase".
    .agg(sum($"Total_Price") as "Total_Purchase")
     .persist()   //  Caching UserPurchase Contents.
    UserPurchase.take(10).foreach(println)   // Printing First 10 Items.
    //  Saving the joined UserPurchase to storage.
    UserPurchase.write.csv(args(2)+"/out.csv")
    //  Stopping the Spark Session.
    ss.stop()
  }
}

