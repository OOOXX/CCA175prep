package com.oooxxx.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.log4j._

object TransformationsExample {
  
  def main(args: Array[String]) {
      
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("TransformationExample").setMaster("local[*]")
  val sc = new SparkContext(conf)
  
  println("Cartesian---Cartesian---Cartesian")
  val x = sc.parallelize(List(1,2,3,4,5))
  val y = sc.parallelize(List(6,7,8,9,10))
  x.cartesian(y).collect().foreach(println)
  
  println("Cogroup---Cogroup---Cogroup")
  val a = sc.parallelize(List((1, "apple"), (2, "banana"), (3, "orange"), (4, "kiwi")),2)
  val b = sc.parallelize(List((1, "apple"), (5, "computer"), (1, "laptop"), (1, "desktop"), (4, "iPad")), 2)
  a.cogroup(b).collect().foreach(println)
  
  println("Subtract---Subtract---Subtract")
  val diff = a.subtract(b)
  diff.collect().foreach(x => println(x._2))
  
  println("CollectAsMap---CollectAsMap---CollectAsMap")
  val c = sc.parallelize(List(1,2,1,3),2)
  val c2 = sc.parallelize(List(5,6,5,7),2)
  val d = c.zip(c2)
  d.collectAsMap.foreach(println)
  d.collect.foreach(println)
  
  println("CombineByKey---CombineByKey---CombineByKey")
  val a1 = sc.parallelize(List("dog", "cat", "gnu", "salmon", "rabbit", "turkey", "wolf", "bear", "bee"), 3)
  val b1 = sc.parallelize(List(1, 1, 2, 2, 2, 1, 2, 2, 2), 3)
  val c1 = a1.zip(b1)
//  val d1 = c1.combineByKey(List(_), (x: List[String], y: String) => y :: x, (x: List[String], y: List[String]) => x ::: y)
//  d1.collect.foreach(f => println(f))
  
  println("FilterByRange---FilterByRange---FilterByRange")
  val randRDD = sc.parallelize(List((2, "cat"), (6, "mouse"), (7, "cup"), (3, "book"), (4, "tv"), (1, "screen"), (5, "heater")), 3)
  val sortedRDD = randRDD.sortByKey()
  sortedRDD.filterByRange(1,3).foreach(println)
  
  }
}
