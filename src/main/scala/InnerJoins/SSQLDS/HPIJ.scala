package InnerJoins.SSQLDS
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
//  Initializing the DS Hash-Partitioned Inner-Join.
object HPIJ {
  // -Creating Users Case Class Definition...
  case class Users(User_ID: Long, First_Name: String, Last_name: String, DOP: String, Address:String, City: String, State_Abbr: String, Zip_Code: String, Country: String, Phone_Number: String, User_Name: String, Password: String, Email: String)
  // -Creating Purchases Case Class Definition...
  case class Purchases(Invoice_ID: Long, User_ID: Long, Product: String, Unit_Amount: Int, Unit_Price: Float)
  //  Defining the main method.
  def main(args: Array[String]): Unit = {
    // Setting the logger Levels...
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    // Initializing Spark Session
    val ss = SparkSession
      .builder
      .appName("DS_HPIJ")
      .getOrCreate()
    // To use spark session implicit tools like the "$" operator...
    import ss.implicits._
    // Importing Users and Purchases Schema...
    val UserSchema = Encoders.product[Users].schema
    val ProductsSchema = Encoders.product[Purchases].schema
    // loading Users Dataset using the Users Case Class and Schema...
    // Considering the header as Column Names that will be used later...
    val InUserDS= ss.read.option("header", "true")
      .schema(UserSchema)   // Schema to apply.
      .csv(args(0))  // Source Connector.
      .as[Users]  // Case Class to apply.
      // -Select the Wanted Columns From the Users Dataset
      // --Concatenating First and Last Names as "Full_Name"...
      .select($"User_ID", concat($"First_Name", lit(" "), $"Last_name") as "Full_Name")
      // Hash Repartitioning...
      .repartition(200, $"User_ID")
      .persist() // Caching the Dataset for further use.
    InUserDS.show(10)  // Printing the first 10 tuples.
    InUserDS.printSchema()  // Printing the Dataset Schema.
    // loading Purchase Dataset using the Users Case Class and Schema...
    // Considering the header as Column Names that will be used later...
    val InPurchseDS = ss.read.option("header","true")
      .schema(ProductsSchema)  // Schema to apply.
      .csv(args(1))  // Source Connector.
      .as[Purchases]  // Case Class to apply.
      // -Select the Wanted Columns From the Purchases Dataset
      // --Multiplying unit amount and unit price as "Total_Price"...
      .select($"User_ID", $"Unit_Amount" * $"Unit_Price" as "Total_Price")
      // -Grouping by both User Id and the full name...
      .groupBy($"User_ID")
      // -Aggregating through the grouped values summing all of them as "Total_Purchase".
      .agg(sum($"Total_Price") as "Total_Purchase")
      // Hash Repartitioning...
      .repartition(200, $"User_ID")
      .persist()  // Caching the Dataset for further use.
    InPurchseDS.show(10)  // Printing the first 10 tuples.
    InPurchseDS.printSchema() // Printing the Dataset Schema.
    //  Joining the two Datasets...
    val UserPurchase = InUserDS.join(InPurchseDS,"User_ID")
      .persist()  // Caching the Dataset for further use.
    UserPurchase.show(10) // Printing the first 10 tuples.
    UserPurchase.printSchema()  // Printing the Dataset Schema.
    // Saving the Dataset into storage.
    UserPurchase.write.csv(args(2)+"/out.csv")
    //  Stopping the Spark Session.
    ss.stop()
  }
}

