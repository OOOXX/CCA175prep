package com.oooxxx.spark

import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.{SparkConf, SparkContext}


object flatMap {
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val conf = new SparkConf().setAppName("flatMap").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val x = sc.parallelize(List("Spark Map Example", "Spark flatMap Example"))
    
    val xFlatMap = x.flatMap(x => x.split(" "))
    xFlatMap.foreach(println)
    
  }
  
}
