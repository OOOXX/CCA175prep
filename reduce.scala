package com.oooxxx.spark

import org.apache.spark._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j._

object Reduce {
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val conf = new SparkConf().setAppName("Reduce").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    val distfile = sc.textFile("/home/OOOXX/Input/Data/log.txt")
    val fil = distfile.map(x => x.split(" ").length)
    val rdd = fil.reduce((a, b) => a + b)
    println(rdd)
  }

  
}
