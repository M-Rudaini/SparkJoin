package InnerJoins.SCoreRDD
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
//  Initializing the RDD Full-Processed Inner-Join Using Reduce by Key.
object FPIJ_RBK {
  // Defining the main method.
  def main(args: Array[String]): Unit = {
    // Setting the Logger levels.
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    // Creating the Spark Core Configuration.
    val conf = new SparkConf()
      .setAppName("RDD_FPIJ_RBK")
    // Creating Spark Context using Spark Core Configuration.
    val sc =new SparkContext(conf)
    //  -Load Users File.
    val InUserFile = sc.textFile(args(0))
      //  -Filtering the Header Line.
      .filter(!_.contains("User_ID"))
      //  -Mapping Remaining Lines.
      //  --Splitting line Values.
      .map(line => line.split(","))
      //  --Taking User ID value (index 0).
      //  --Merging First Name (index 1), Space “ “, and Last Name (index 2).
      //  -Ignoring unwanted values.
      .map(Array => (Array(0), Array(1)+" "+Array(2)))
      .persist()    //  Caching.
    // Printing the first 10 lines.
    InUserFile.take(10).foreach(println)
    //  -Load Purchase File.
    val InPurchaseFile = sc.textFile(args(1))
      //  -Filtering the Header Line.
      .filter(!_.contains("User_ID"))
      //  -Mapping Remaining Lines.
      //  --Splitting line Values.
      .map(line => line.split(","))
      //  --Taking User ID value (index 1).
      //  --Multiplying No of Units (index 3) by Unit Price (index 4).
      .map(Array => (Array(1), Array(3).toFloat*Array(4).toFloat))
      //  -Reducing by key (UserID).
      //  -Sum all the Reduced values.
      .reduceByKey(_+_)
      .persist()   //  Caching.
    // Printing the first 10 lines.
    InPurchaseFile.take(10).foreach(println)
    // Joining the two RDDs.
    val UserPurchase = InUserFile.join(InPurchaseFile)
      .persist()   //  Caching.
    //   Release the Cache of used RDDs. 
    InUserFile.unpersist()
    InPurchaseFile.unpersist()
    // Printing the first 10 lines.
    UserPurchase.take(10).foreach(println)
    //  Saving the joined data into storage.
    UserPurchase.saveAsTextFile(args(2))
    //  Stopping Spark Context.
    sc.stop()
  }
}

