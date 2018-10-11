package InnerJoins.Test

import org.apache.log4j._
import org.apache.spark.sql._
import ScalaWriter.Writer.Write
import org.apache.spark.{HashPartitioner, RangePartitioner}


object RDDProcess {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val ss = SparkSession.builder.master("local[2]").appName("SparkSQLDFjoin")
      .getOrCreate()
    val t0 = System.currentTimeMillis()

    val InPurchaseFileDGBK = ss.sparkContext.textFile(args(0)).filter(!_.contains("User_ID"))
      .map(line => (line.split(",")(1), (line.split(",")(3).toFloat*line.split(",")(4).toFloat).toInt))
      .groupByKey().map(x => (x._1,x._2.sum))
    InPurchaseFileDGBK.take(10).foreach(println)
    val t1 = System.currentTimeMillis()
    val DGBK = t1-t0
    println("RDD Direct GBK: " , DGBK)


    val t2 = System.currentTimeMillis()

    val InPurchaseFileDRBK = ss.sparkContext.textFile(args(0))
      .filter(!_.contains("User_ID"))
      .map(line => (line.split(",")(1), (line.split(",")(3).toFloat*line.split(",")(4).toFloat).toInt))
      .reduceByKey(_+_)
    InPurchaseFileDRBK.take(10).foreach(println)
    val t3 = System.currentTimeMillis()
    val DRBK = t3-t2
    println("RDD Direct RBK: " , DRBK)

    val t4 = System.currentTimeMillis()

    val RDD1 = ss.sparkContext.textFile(args(0))
    val RDD2 = RDD1.filter(!_.contains("User_ID"))
    val RDD3 = RDD2.map(line => (line.split(",")(1), (line.split(",")(3).toFloat*line.split(",")(4).toFloat).toInt))
    val RDD4 = RDD3.groupByKey()
    val InPurchaseFileSGBK = RDD4.map(x => (x._1,x._2.sum))
    InPurchaseFileSGBK.take(10).foreach(println)
    val t5 = System.currentTimeMillis()
    val SGBK = t5-t4
    println("RDD Stepper GBK: " , SGBK)


    val t6 = System.currentTimeMillis()

    val RDD5 = ss.sparkContext.textFile(args(0))
    val RDD6 = RDD5.filter(!_.contains("User_ID"))
    val RDD7 = RDD6.map(line => (line.split(",")(1), (line.split(",")(3).toFloat*line.split(",")(4).toFloat).toInt))
    val InPurchaseFileSRBK = RDD7.reduceByKey(_+_)
    InPurchaseFileSRBK.take(10).foreach(println)
    val t7 = System.currentTimeMillis()
    val SRBK = t7-t6
    println("RDD Stepper RBK: " , SRBK)

    val t8 = System.currentTimeMillis()

    val RDD8 = ss.sparkContext.textFile(args(0))
    val RDD9 = RDD8.filter(!_.contains("User_ID"))
    val RDD10 = RDD9.map(line => (line.split(",")(1), (line.split(",")(3).toFloat*line.split(",")(4).toFloat).toInt))
    val PPart1 = new HashPartitioner(RDD10.partitions.length)
    val RDD11 = RDD10.groupByKey(PPart1)
    val InPurchaseFileHSGBK = RDD11.map(x => (x._1,x._2.sum))
    InPurchaseFileHSGBK.take(10).foreach(println)
    val t9 = System.currentTimeMillis()
    val HSGBK = t9-t8
    println("RDD Hash Partitioned Stepper GBK: " , HSGBK)


    val t10 = System.currentTimeMillis()

    val RDD12 = ss.sparkContext.textFile(args(0))
    val RDD13 = RDD12.filter(!_.contains("User_ID"))
    val RDD14 = RDD13.map(line => (line.split(",")(1), (line.split(",")(3).toFloat*line.split(",")(4).toFloat).toInt))
    val PPart2 = new HashPartitioner(RDD14.partitions.length)
    val InPurchaseFileHSRBK = RDD14.reduceByKey(PPart2,_+_)
    InPurchaseFileHSRBK.take(10).foreach(println)
    val t11 = System.currentTimeMillis()
    val HSRBK = t11-t10
    println("RDD Hash Partitioned Stepper RBK: " , HSRBK)

    val t12 = System.currentTimeMillis()

    val RDD15 = ss.sparkContext.textFile(args(0))
    val RDD16 = RDD15.filter(!_.contains("User_ID"))
    val RDD17 = RDD16.map(line => (line.split(",")(1), (line.split(",")(3).toFloat*line.split(",")(4).toFloat).toInt))
    val PPart3 = new RangePartitioner(100,RDD17)
    val RDD18 = RDD17.groupByKey(PPart3)
    val InPurchaseFileRSGBK = RDD18.map(x => (x._1,x._2.sum))
    InPurchaseFileRSGBK.take(10).foreach(println)
    val t13 = System.currentTimeMillis()
    val RSGBK = t13-t12
    println("RDD Range Partitioned Stepper GBK: " , RSGBK)


    val t14 = System.currentTimeMillis()

    val RDD19 = ss.sparkContext.textFile(args(0))
    val RDD20 = RDD19.filter(!_.contains("User_ID"))
    val RDD21 = RDD20.map(line => (line.split(",")(1), (line.split(",")(3).toFloat*line.split(",")(4).toFloat).toInt))
    val PPart4 = new RangePartitioner(100,RDD21)
    val InPurchaseFileRSRBK = RDD21.reduceByKey(PPart4,_+_)
    InPurchaseFileRSRBK.take(10).foreach(println)
    val t15 = System.currentTimeMillis()
    val RSRBK = t15-t14
    println("RDD Range Partitioned Stepper RBK: " , RSRBK)

    val line = DGBK+","+DRBK+","+SGBK+","+SRBK+","+HSGBK+","+HSRBK+","+RSGBK+","+RSRBK

    Write(line, "RDDProcess.csv")

    ss.stop()
  }
}

