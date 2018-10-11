package InnerJoins.SSQLDS

import ScalaWriter.Writer.Write
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object HPIJ {

  //-Creating Users Case Class Definition...
  case class Users(User_ID: Long, First_Name: String, Last_name: String, DOP: String,
                   Address:String, City: String, State_Abbr: String, Zip_Code: String,
                   Country: String, Phone_Number: String, User_Name: String,
                   Password: String, Email: String)


  //-Creating Purchases Case Class Definition...
  case class Purchases(Invoice_ID: Long, User_ID: Long, Product: String,
                       Unit_Amount: Int, Unit_Price: Float)

  def main(args: Array[String]): Unit = {
    //Setting log Levels...
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    //intializing Spark Session
    val ss = SparkSession.builder.master("local[2]")
      .appName("DS_HPIJ")
      .getOrCreate()

    //To use spark session implicit tools like the "$" operator...
    import ss.implicits._

    //Importing Users and Purchases Schema...
    val UserSchema = Encoders.product[Users].schema
    val ProductsSchema = Encoders.product[Purchases].schema

    //Current time point log..
    val t0 = System.currentTimeMillis()

    //loading Users Dataset using the Users Case Class and Schema...
    val InUserDS= ss.read.option("header", "true").schema(UserSchema).csv(args(0)).as[Users]
    InUserDS.show(10)
    InUserDS.printSchema()

    //loading Purchase Dataset using the Users Case Class and Schema...
    val InPurchseDS = ss.read.option("header","true").schema(ProductsSchema).csv(args(1)).as[Purchases]
    InPurchseDS.show(10)
    InPurchseDS.printSchema()

    val t1 = System.currentTimeMillis()

    //Hash Repartitioning...
    val PUserDS = InUserDS.repartition(200, $"User_ID")

    //Hash Repartitioning...
    val PPurchaseDS = InPurchseDS.repartition(200, $"User_ID")

    //-Select the Wanted Columns From the Users Dataset
    //--Concatenating First and Last Names as "Full_Name"...
    val UDS1 = PUserDS.select($"User_ID", concat($"First_Name", lit(" "), $"Last_name") as "Full_Name")
    UDS1.show(1)
    UDS1.printSchema()

    //-Select the Wanted Columns From the Purchases Dataset
    //--Multiplying unit amount and unit price as "Total_Price"...
    val PDS1 = PPurchaseDS.select($"User_ID", $"Unit_Amount" * $"Unit_Price" as "Total_Price")
    PDS1.show(1)
    PDS1.printSchema()

    //-Grouping by both User Id and the full name...
    val PDS2 = PDS1.groupBy($"User_ID")

    //-Aggrigating through the grouped values summing all of them as "Total_Purchase".
    val PDS3 =PDS2.agg(sum($"Total_Price") as "Total_Purchase")

    PDS3.show(1)
    PDS3.printSchema()

    val t2 = System.currentTimeMillis()

    // Joining the two DataFrames...
    val UserPurchase = UDS1.join(PDS3,"User_ID")
    UserPurchase.show(10)
    UserPurchase.printSchema()

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
    Write(Line,"DS.HPIJ.csv")

    ss.stop()
  }
}
