package InnerJoins.SCoreRDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
//to Write time
import ScalaWriter.Writer.Write

// Initializing the Hash Partitioned Inner-Join Using Group By Key..
object HPIJ_GBK {
  //Defining the main method..
  def main(args: Array[String]): Unit = {
    //Setting logining levels..
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    //Creating the Spark Core Configuration..
    val conf = new SparkConf().setMaster("local[*]")
      .setAppName("RDD_HPIJ_GBK")
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
    val URDD1 =  InUserFile.filter(!_.contains("User_ID"))

    // -Mapping Remaining Lines
    // --Splitting line Values
    val URDD2 = URDD1.map(line => line.split(","))

    // --Taking User ID value (index 0),
    // --Concating First (index 1) and Last (index 2) names with Space between them
    // -Ignoring unwanted values
    val URDD3 = URDD2.map(Array => (Array(0), Array(1)+" "+Array(2)))

    //Creating the Hash Partitioner that Creates 200 Partitions
    val Hpart = new HashPartitioner(200)

    //Partitioning the (key, value) mapped User RDD using the Hash Partitioner
    val UserRDD = URDD3.partitionBy(Hpart).persist()
    //Printing the first 10 lines..
    UserRDD.take(10).foreach(println)

    // -Filtering the Header Line
    val PRDD1 = InPurchaseFile.filter(!_.contains("User_ID"))

    // -Mapping Remaining Lines
    // --Splitting line Values
    val PRDD2 = PRDD1.map(line => line.split(","))

    // --Taking User ID value (index 1),
    // --Multiplying No of Units (index 3) by Unit Price (index 4)
    val PRDD3 = PRDD2.map(Array => (Array(1), Array(3).toFloat*Array(4).toFloat))

    //Partitioning the (key, value) mapped Purchase RDD using the Hash Partitioner
    val PRDD4 = PRDD3.partitionBy(Hpart).persist()
    // -Grouping by key (UserID)
    val PRDD5 = PRDD4.groupByKey()

    // -Keeping the key and sum all the grouped values.
    // -Ignoring unwanted values
    val PurchaseRDD = PRDD5.mapValues(values => values.sum)

    //Printing the first 10 lines..
    PurchaseRDD.take(10).foreach(println)

    val t2 = System.currentTimeMillis()
    //Joining the two RDDs...
    val UserPurchase = UserRDD.join(PurchaseRDD)

    //Printing the first 10 lines..
    UserPurchase.take(10).foreach(println)

    val t3 = System.currentTimeMillis()

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
    Write(Line,"RDD.HPIJ.GBK.csv")

    sc.stop()
  }
}
