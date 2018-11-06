package InnerJoins.SSQLDF
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
//  Initializing the DF Semi-Processed Inner-Join.
object SPIJ {
  //  Defining the main method.
  def main(args: Array[String]): Unit = {
    // Setting the logger Levels...
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    // Initializing Spark Session
        val ss = SparkSession
          .builder
          .appName("DF_SPIJ")
          .getOrCreate()
    // To use spark session implicit tools like the "$" operator...
    import ss.implicits._
    // loading Users DataFrame...
    // Considering the header as Column Names that will be used later...
    val InUserDF = ss.read.option("header","true")
     .csv(args(0))  // Source Connector.
      // Selecting the Required Columns from the Users DataFrame...
      .select($"User_ID", $"First_Name", $"Last_Name")
      .persist()   //  Caching the DataFrame contents for further use...
    InUserDF.show(10)
    // loading Purchases DataFrame...
    // Considering the header as Column Names that will be used later...
    val InPurchseDF = ss.read.option("header","true")
      .csv(args(1))  // Source Connector.
      // Selecting the Required Columns from the Users DataFrame...
      //  to not conflict with the one in the Users DataFrame...
      .select($"User_ID", $"Unit_Amount", $"Unit_Price")
      .persist()  //  Caching the DataFrame contents for further use...
    // Print the first 10 tuples...
    InPurchseDF.show(10)
    //  Joining the two DataFrames...
    val UserPurchase = InUserDF.join(InPurchseDF, "User_ID")
     .persist()  //  Caching DataFrame Contents.
      // Grouping the Joined DataFrame
      // Concatenate first and last names as new column "Full_Name"...
      .groupBy($"User_ID",concat($"First_Name", lit(" "), $"Last_Name")as "Full_Name")
      // Aggregating:
      // -Multiply the unit amount by the unit price as new column "Total_Price"...
      // -Sum all total prices as "Total_Purchases"...
      .agg(sum($"Unit_Amount" * $"Unit_Price" as "Total_Price") as "Total_Purchase")
      .persist()   //  Caching DataFrame Contents.
    // Print the first 10 tuples...
    UserPurchase.show(10)
    //  Saving the joined DataFrame to storage.
    UserPurchase.write.csv(args(2)+"/out.csv")
    //  Stopping the Spark Session.
    ss.stop()
  }
}

