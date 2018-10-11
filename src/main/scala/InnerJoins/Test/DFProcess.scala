package InnerJoins.Test

import ScalaWriter.Writer.Write
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, sum}


object DFProcess {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val ss1 = SparkSession.builder.master("local[2]").appName("SparkSQLDFjoin")
      .getOrCreate()
    import ss1.implicits._

    val t2 = System.currentTimeMillis()

    val DF1 = ss1.read.option("header","true").csv(args(0))
    val DF2 = DF1.select($"User_ID" as "ID", $"Unit_Amount" * $"Unit_Price" as "Total_Price")
    val DF3 = DF2.groupBy("ID")
    val InPurchaseSDF = DF3.agg(sum("Total_Price") as "Total_Purchase")

    val t3 = System.currentTimeMillis()
    InPurchaseSDF.show(10)
    val SDF = t3-t2
    println("Stepper DF: " , SDF)
    ss1.stop()


    val ss2 = SparkSession.builder.master("local[2]").appName("SparkSQLDFjoin")
      .getOrCreate()
    val t4 = System.currentTimeMillis()

    val DF4 = ss2.read.option("header","true").csv(args(0))
    val DF5 = DF4.repartition(200, col("User_ID"))
    val DF6 = DF5.select($"User_ID" as "ID", $"Unit_Amount" * $"Unit_Price" as "Total_Price")
    val DF7 = DF6.groupBy($"ID")
    val InPurchaseHSDF = DF7.agg(sum("Total_Price") as "Total_Purchase")

    val t5 = System.currentTimeMillis()
    InPurchaseHSDF.show(10)
    val HSDF = t5-t4
    println("Hash Partitioned Stepper DF: " , HSDF)
    ss2.stop()

    val ss3 = SparkSession.builder.master("local[2]").appName("SparkSQLDFjoin")
      .getOrCreate()
    val t6 = System.currentTimeMillis()

    val DF8 = ss3.read.option("header","true").csv(args(0))
    val DF9 = DF8.repartitionByRange(200, $"User_ID")
    val DF10 = DF9.select($"User_ID" as "ID", $"Unit_Amount" * $"Unit_Price" as "Total_Price")
    val DF11 = DF10.groupBy($"ID")
    val InPurchaseRSDF = DF11.agg(sum("Total_Price") as "Total_Purchase")

    val t7 = System.currentTimeMillis()
    InPurchaseRSDF.show(10)
    val RSDF = t7-t6
    println("Range Partitioned Stepper DF: " , RSDF)
    ss3.stop()


    val ss4 = SparkSession.builder.master("local[2]").appName("SparkSQLDFjoin")
      .getOrCreate()
    val t0 = System.currentTimeMillis()

    val InPurchaseDDF = ss4.read.option("header","true").csv(args(0))
      .select($"User_ID" as "ID", $"Unit_Amount" * $"Unit_Price" as "Total_Price")
      .groupBy("ID").agg(sum("Total_Price") as "Total_Purchase")

    val t1 = System.currentTimeMillis()
    InPurchaseDDF.show(10)
    val DDF = t1-t0
    println("Direct DF: " , DDF)
    ss4.stop()

    val ss5 = SparkSession.builder.master("local[2]").appName("SparkSQLDFjoin")
      .getOrCreate()
    val t8 = System.currentTimeMillis()

    val InPurchaseHDDF = ss5.read.option("header","true").csv(args(0))
      .repartition(200, $"User_ID")
      .select($"User_ID" as "ID", $"Unit_Amount" * $"Unit_Price" as "Total_Price")
      .groupBy("ID").agg(sum("Total_Price") as "Total_Purchase")

    val t9 = System.currentTimeMillis()
    InPurchaseHDDF.show(10)
    val HDDF = t9-t8
    println("Hash Partitioned Direct DF: " , HDDF)
    ss5.stop()

    val ss6 = SparkSession.builder.master("local[2]").appName("SparkSQLDFjoin")
      .getOrCreate()
    val t10 = System.currentTimeMillis()

    val InPurchaseRDDF = ss6.read.option("header","true").csv(args(0))
      .repartitionByRange(200, $"User_ID")
      .select($"User_ID" as "ID", $"Unit_Amount" * $"Unit_Price" as "Total_Price")
      .groupBy("ID").agg(sum("Total_Price") as "Total_Purchase")

    val t11 = System.currentTimeMillis()
    InPurchaseRDDF.show(10)
    val RDDF = t11-t10
    println("Range Partitioned Direct DF: " , RDDF)
    ss6.stop()


    val line = DDF+","+HDDF+","+RDDF+","+SDF+","+HSDF+","+RSDF

    Write(line, "DFProcess.csv")
  }
}

