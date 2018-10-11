package InnerJoins.Test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import System.gc

import ScalaWriter.Writer.Write
object DDFProcess {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val ss = SparkSession.builder.master("local[2]").appName("DDF Process")
      .getOrCreate()
    import ss.implicits._

    val t0 = System.currentTimeMillis()
    val InPurchaseDDF1 = ss.read.option("header","true").csv(args(0))
      .select($"User_ID" as "ID", $"Unit_Amount" * $"Unit_Price" as "Total_Price")
      .groupBy("ID").agg(sum("Total_Price") as "Total_Purchase")

    val t1 = System.currentTimeMillis()
    InPurchaseDDF1.show(10)
    val DDF1 = t1-t0
    println("Direct DF: " , DDF1)

    val t2 = System.currentTimeMillis()
    val InPurchaseDDF2 = ss.read.option("header","true").csv(args(0))
      .select($"User_ID" as "ID", $"Unit_Amount" * $"Unit_Price" as "Total_Price")
      .groupBy("ID").agg(sum("Total_Price") as "Total_Purchase")

    val t3 = System.currentTimeMillis()
    InPurchaseDDF2.show(10)
    val DDF2 = t3-t2
    println("Direct DF: " , DDF2)

    val t4 = System.currentTimeMillis()
    val InPurchaseDDF3 = ss.read.option("header","true").csv(args(0))
      .select($"User_ID" as "ID", $"Unit_Amount" * $"Unit_Price" as "Total_Price")
      .groupBy("ID").agg(sum("Total_Price") as "Total_Purchase")

    val t5 = System.currentTimeMillis()
    InPurchaseDDF3.show(10)
    val DDF3 = t5-t4
    println("Direct DF: " , DDF3)

    ss.stop()
    val line = DDF1+","+DDF2+","+DDF3

    Write(line, "DDFProcess.csv")
  }

}
