package com.oooxxx.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object MovingAverage {
  
  def main(args: Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
    .master("local")
    .appName("MovingAverage")
    .config("spark.some.config.option", "some-vale")
    .getOrCreate()
    
    import spark.implicits._
    
    val customers = spark.sparkContext.parallelize(List(
      ("Alice", "2016-05-01", 50.00),
      ("Alice", "2016-05-03", 45.00),
      ("Alice", "2016-05-04", 55.00),
      ("Bob", "2016-05-01", 25.00),
      ("Bob", "2016-05-04", 29.00),
      ("Bob", "2016-05-06", 27.00))).toDF("name", "date", "amountSpent");
    
    val spec_1 = Window.partitionBy("name").orderBy("date").rowsBetween(-1, 1)
    
    customers.withColumn("movingAverage", avg("amountSpent") over spec_1).show()
    
    val spec_2 = Window.partitionBy("name").orderBy("date").rowsBetween(Long.MinValue, 0)
    
    println("*******")
    customers.withColumn("cumsum", sum("amountSpent") over spec_2).show()
    
    val spec_3 = Window.partitionBy("name").orderBy("date")
    
    println("*******")
    customers.withColumn("prevousAmoutSpent", lag(customers("amountSpent"), 1) over spec_3).show()
    
    println("*******")
    customers.withColumn("rank", rank() over spec_3).show()
    
    
  }
  
}
