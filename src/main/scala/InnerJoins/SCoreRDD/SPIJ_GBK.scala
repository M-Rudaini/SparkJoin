1.	package InnerJoins.SCoreRDD
2.	import org.apache.log4j.{Level, Logger}
3.	import org.apache.spark._
4.	//  Initializing the RDD Semi-Processed Inner-Join Using Group by Key.
5.	object SPIJ_GBK {
6.	  // Defining the main method.
7.	  def main(args: Array[String]): Unit = {
8.	    // Setting the logger levels.
9.	    Logger.getLogger("org").setLevel(Level.ERROR)
10.	    Logger.getLogger("akka").setLevel(Level.ERROR)
11.	    // Creating the Spark Core Configuration.
12.	    val conf = new SparkConf()
13.	      .setAppName("RDD_SPIJ_GBK")
14.	    // Creating Spark Context using Spark Core Configuration.
15.	    val sc =new SparkContext(conf)
16.	    //  -Load Users File.
17.	    val InUserFile = sc.textFile(args(0))
18.	    //  -Filtering the Header Line.
19.	    .filter(!_.contains("User_ID"))
20.	    //  -Mapping Remaining Lines.
21.	    //  --Splitting line Values.
22.	    .map(line => line.split(","))
23.	    //  --Taking User ID value (index 0).
24.	    //  --Merging First Name (index 1), Space “ “, and Last Name (index 2).
25.	    .map(Array => (Array(0), Array(1)+" "+Array(2)))
26.	      .persist()   //  Caching.
27.	    // Printing the first 10 lines.
28.	    InUserFile.take(10).foreach(println)
29.	    //  -Load Purchase File.
30.	    val InPurchaseFile = sc.textFile(args(1))
31.	    //  -Filtering the Header Line.
32.	    .filter(!_.contains("User_ID"))
33.	    //  -Mapping Remaining Lines.
34.	    //  --Splitting line Values.
35.	    .map(line => line.split(","))
36.	    //  --Taking User ID value (index 1).
37.	    //  --Multiplying No of Units (index 3) by Unit Price (index 4).
38.	    //  -Ignoring unwanted values.
39.	    .map(Array => (Array(1), Array(3).toFloat*Array(4).toFloat))
40.	      .persist() //  Caching.
41.	    // Printing the first 10 lines.
42.	    InPurchaseFile.take(10).foreach(println)
43.	    // Joining the two RDDs.
44.	    val UserPurchase = InUserFile.join(InPurchaseFile).persist()
45.	    // Grouping by Key.
46.	    .groupByKey()
47.	    // Mapping Values.
48.	    .mapValues(x => {
49.	      var sum=0.0
50.	      var name=""
51.	      // taking the Full Name from the First part of the Head of the Iterable.
52.	      name = x.head._1
53.	      // Summing the whole second parts of the Iterables.
54.	      x.foreach(i => {
55.	        sum += i._2
56.	      })
57.	      (name,sum)
58.	    }).persist() //  Caching.
59.	    // Printing the first 10 lines.
60.	    UserPurchase.take(10).foreach(println)
61.	    //  Saving joined data to file Storage.
62.	    UserPurchase.saveAsTextFile(args(2))
63.	    //  Stopping Spark Context.
64.	    sc.stop()
65.	  }
66.	}
