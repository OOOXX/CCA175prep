package com.oooxxx.spark

import org.apache.spark.sql.SparkSession
import org.apache.log4j._

object JSONReader {
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sqlContext = SparkSession.builder()
    .master("local")
    .appName("JSONReader")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

    
    val path = "/home/OOOXX/Input/Data/sales.json"
    val salesDF = sqlContext.read.json(path)
    salesDF.show()
    salesDF.createOrReplaceTempView("sales")
    
    val aggDF = sqlContext.sql("SELECT SUM(amountPaid) FROM sales")
    println(aggDF.collectAsList())
    
    val result = sqlContext.sql("SELECT customerID, itemName FROM sales ORDER BY itemName")
    result.show()
       
  }
  
}
