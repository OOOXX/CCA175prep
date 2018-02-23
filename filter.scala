package com.oooxxx.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j._

object Filter {
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("Filter").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    val x = sc.parallelize(List("Transformation demo", "Test demo", "Filter demo", "Spark is powerfull", 
        "Spark is faster", "Spark is in memory"));
    
    val line1 = x.filter(line => line.contains("Spark") || line.contains("Transformation"))
    line1.collect().foreach(println)
    
    val line2 = x.filter(line => !line.contains("Filter"))
    println("********************************************")
    line2.collect().foreach(println)
    
    val line3 = x.filter(line => line.contains("Spark")).count()
    println("********************************************")
    println("Count of Spark lines is: " + line3)
  
   
  }
  
}
