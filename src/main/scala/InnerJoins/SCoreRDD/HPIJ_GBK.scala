package InnerJoins.SCoreRDD
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
//  Initializing the Hash Partitioned Inner-Join Using Group by Key.
object HPIJ_GBK {
  // Defining the main method.
  def main(args: Array[String]): Unit = {
    // Setting the java logger levels.
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    // Creating the Spark Core Configuration.
    val conf = new SparkConf()
      .setAppName("RDD_HPIJ_GBK")
    // Creating Spark Context using Spark Core Configuration.
    val sc =new SparkContext(conf)
    // Creating the Hash Partitioner that Creates 200 Partitions
    val Hpart = new HashPartitioner(200)
    //  -Load Users File.
    val InUserFile = sc.textFile(args(0))
      //  -Filtering the Header Line.
      .filter(!_.contains("User_ID"))
      //  -Mapping Remaining Lines.
      //  --Splitting line Values.
      .map(line => line.split(","))
      //  --Taking User ID value (index 0).
      //  --Merging First Name (index 1), Space “”, and Last Name (index 2).
      //  -Ignoring unwanted values.
      .map(Array => (Array(0), Array(1)+" "+Array(2)))
      // Partitioning the (k, v) mapped User RDD using the Hash Partitioner.
      .partitionBy(Hpart)
      .persist() //  Caching.
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
      // Partitioning the (k, v) mapped Purchase RDD using the Hash Partitioner.
      .partitionBy(Hpart)
       //  Grouping the mapped values by key (User ID).
      .groupByKey()
       //  Mapping grouped values to take their total sum.
      .mapValues(values => values.sum)
      .persist() //  Caching.
    // Printing the first 10 lines.
    InPurchaseFile.take(10).foreach(println)
    // Joining the two RDDs.
    val UserPurchase = InUserFile.join(InPurchaseFile)
    //  Releasing the cache of used RDDs.
    InUserFile.unpersist()
    InPurchaseFile.unpersist()
    // Printing the first 10 lines.
    UserPurchase.take(10).foreach(println)
    //  Saving the Joined Data into the storage.
    UserPurchase.saveAsTextFile(args(2))
    //  Stopping the Spark Context.
    sc.stop()
  }
}

