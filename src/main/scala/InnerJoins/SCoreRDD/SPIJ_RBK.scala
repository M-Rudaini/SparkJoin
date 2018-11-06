1.	package InnerJoins.SCoreRDD
2.	import org.apache.log4j.{Level, Logger}
3.	import org.apache.spark._
4.	//  Initializing the RDD Semi-Processed Inner-Join Using Reduce by Key.
5.	object SPIJ_RBK {
6.	    // Defining the main method.
7.	    def main(args: Array[String]): Unit = {
8.	         // Setting the logger levels.
9.	        Logger.getLogger("org").setLevel(Level.ERROR)
10.	        Logger.getLogger("akka").setLevel(Level.ERROR)
11.	        // Creating the Spark Core Configuration.
12.	        val conf = new SparkConf().setAppName("RDD_SPIJ_RBK")
13.	       // -Creating Spark Context using Spark Core Configuration.
14.	       val sc =new SparkContext(conf)
15.	       //  -Load Users File.
16.	       val InUserFile = sc.textFile(args(0))
17.	          //  -Filtering the Header Line.
18.	          .filter(!_.contains("User_ID"))
19.	          //  -Mapping Remaining Lines.
20.	          //  --Splitting line Values.
21.	          .map(line => line.split(","))
22.	          //  --Taking User ID value (index 0).
23.	          //  --Merging First Name (index 1), Space “ “, and Last Name (index 2).
24.	         .map(Array => (Array(0), Array(1)+" "+Array(2)))
25.	        .persist() //  Caching.
26.	      // Printing the first 10 lines.
27.	      InUserFile.take(10).foreach(println)
28.	    //  -Load Purchase File.
29.	    val InPurchaseFile = sc.textFile(args(1))
30.	    //  -Filtering the Header Line.
31.	    .filter(!_.contains("User_ID"))
32.	    //  -Mapping Remaining Lines.
33.	    //  --Splitting line Values.
34.	    .map(line => line.split(","))
35.	    //  --Taking User ID value (index 1).
36.	    //  --Multiplying No of Units (index 3) by Unit Price (index 4).
37.	    //  -Ignoring unwanted values.
38.	    .map(Array => (Array(1), (Array(3).toFloat*Array(4).toFloat).toInt))
39.	    .persist()     //  Caching.
40.	    // Printing the first 10 lines.
41.	    InPurchaseFile.take(10).foreach(println)
42.	    // Joining the two RDDs.
43.	    val UserPurchase = InUserFile.join(InPurchaseFile)
44.	    // Reducing by Key.
45.	    // Summing all the total prices to get the total purchase for each user.
46.	    .reduceByKey((x,y)=> (x._1, x._2+y._2))
47.	    .persist()      //  Caching.
48.	    //  Printing the first 10 lines.
49.	    UserPurchase.take(10).foreach(println)
50.	    //  Saving the file.
51.	    UserPurchase.saveAsTextFile(args(2))
52.	    //  Stopping the Spark Context.
53.	    sc.stop()
54.	  }
55.	}
