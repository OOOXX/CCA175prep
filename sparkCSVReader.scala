package com.oooxxx.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkCSVReader {
  
  def main(args: Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession.builder()
    .master("local")
    .appName("SparkCSVReader")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
    
    val auctionDF = spark.read
    .format("com.databricks.spark.csv")
    .option("header", true)
    .option("inferSchema", true)
    .load("/home/OOOXX/Input/Data/ebay_1.csv")
    
   // auctionDF.printSchema()
   // auctionDF.select("auctionid", "bidder").show()
    
    val count = auctionDF.select("auctionid").distinct().count()
    println("Distinct auction items: " + count)
    
    auctionDF.groupBy("auctionid", "item").count().sort("auctionid").show()
    
    auctionDF.groupBy("item", "auctionid").count().agg(min("count"), avg("count"), max("count")).show()
    
    auctionDF.filter("price > 100").sort("auctionid").show()
    
    auctionDF.createOrReplaceTempView("auction")
    
    val results = spark.sql("SELECT auctionid, item, count(bid) as BidCount FROM auction GROUP BY auctionid, item")
    
    results.sort("auctionid").show()
    
    spark.sql("SELECT auctionid, item, MAX(price) as MaxPrice FROM auction GROUP BY item, auctionid").sort("auctionid").show()
    
  }
  
}
