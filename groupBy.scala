package com.oooxxx.spark

import org.apache.spark._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j._


object GroupBy {
   
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val conf = new SparkConf().setAppName("GroupBy").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    val words = Array("a", "b", "b", "c", "d", "e", "a", "b", "b", "c", "d", "e", "b", "b", "c", "d", "e")
    val wordsRDD = sc.parallelize(words)    
    val wordsPairedRDD = wordsRDD.map(x => (x,1))
    
    val RDDGroupBy = wordsPairedRDD.groupByKey()
    val RDDKeyCount = RDDGroupBy.map(x => (x._1, x._2.sum)).foreach(println)
    
  }
  
}
