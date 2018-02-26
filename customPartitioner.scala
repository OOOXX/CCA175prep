package com.oooxxx.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.Partitioner

object CustomPartitioner {
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
    .master("local")
    .appName("CustomPartitioner")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
    
    val inputFile = spark.sparkContext.textFile("/home/OOOXX/Input/Data/partitioner.txt")
    val pairRDD = inputFile.flatMap(x => x.split(" ")).map(x => (x, 1))
    val partitionData = pairRDD.partitionBy(new MyCustomerPartitioner(2)).map(x => x._1)
    
    partitionData.foreach(println)
    
    val partitionOutput = partitionData.mapPartitionsWithIndex((partitionIndex, dataIterator) => dataIterator.
        map(dataInfor => dataInfor + " is located in " + partitionIndex + " partition."));
    
    partitionOutput.saveAsTextFile("/home/OOOXX/Input/Data/partitionerOut")
    
  }
  
}

class MyCustomerPartitioner(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int =
    {
      val out = toInt(key.toString)
      out
    }

  override def equals(other: Any): Boolean = other match {
    case dnp: MyCustomerPartitioner =>
      dnp.numPartitions == numPartitions
    case _ =>
      false
  }

  def toInt(s: String): Int =
    {
      try {
        s.toInt
        0
      } catch {
        case e: Exception => 1

      }
    }
}
