package InnerJoins.SCoreRDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import ScalaWriter.Writer.Write //to Write time

// Initializing the Semi-Processed Inner-Join Using Reduce By Key..
object SPIJ_RBK {
  //Defining the main method..
  def main(args: Array[String]): Unit = {
    //Setting logining levels..
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    //Creating the Spark Core Configuration..
    val conf = new SparkConf().setMaster("local[2]")
      .setAppName("RDD_SPIJ_RBK")
    //Creating Spark Context using Spark Core Config..
    val sc =new SparkContext(conf)

    //Current time point log..
    val t0 = System.currentTimeMillis()

    // -Load Users File
    val InUserFile = sc.textFile(args(0))

    // -Load Purchase File
    val InPurchaseFile = sc.textFile(args(1))
    val t1 = System.currentTimeMillis()

    // -Filtering the Header Line
    val RDD1 =  InUserFile.filter(!_.contains("User_ID"))

    // -Mapping Remaining Lines
    // --Splitting line Values
    val RDD2 = RDD1.map(line => line.split(","))

    // --Taking User ID value (index 0),
    // --Concating First (index 1) and Last (index 2) names with Space between them
    // -Ignoring unwanted values
    val UserRDD = RDD2.map(Array => (Array(0), Array(1)+" "+Array(2)))

    //Printing the first 10 lines..
    UserRDD.take(10).foreach(println)

    // -Filtering the Header Line
    val RDD3 = InPurchaseFile.filter(!_.contains("User_ID"))

    // -Mapping Remaining Lines
    // --Splitting line Values
    val RDD4 = RDD3.map(line => line.split(","))

    // --Taking User ID value (index 1),
    // --Multiplying No of Units (index 3) by Unit Price (index 4)
    // -Ignoring unwanted values
    val PurchaseRDD = RDD4.map(Array => (Array(1), (Array(3).toFloat*Array(4).toFloat).toInt))

    //Printing the first 10 lines..
    PurchaseRDD.take(10).foreach(println)

    val t2 = System.currentTimeMillis()
    //Joining the two RDDs...
    val UPRDD1 = UserRDD.join(PurchaseRDD)

    val t3 = System.currentTimeMillis()

    //Reducing By Key...
    val UserPurchase = UPRDD1.reduceByKey((x,y)=> (x._1, x._2+y._2))

    //Printing the first 10 lines..
    UserPurchase.take(10).foreach(println)


    val t4 = System.currentTimeMillis()
    //    UserPurchase.coalesce(1).saveAsTextFile(args(2))

    val t5 = System.currentTimeMillis()
    val TRead = t1-t0
    val TPre = t2-t1
    val TJoin = t3-t2
    val TPost = t4-t3
    val TWrite = t5-t4
    println("Read, Pre-Processing, Join, Post-Processing, Write")
    val Line = TRead+","+TPre+","+TJoin+","+TPost+","+TWrite
    println(Line)
    Write(Line,"RDD.SPIJ.RBK.csv")

    sc.stop()
  }
}