package com.oooxxx.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.log4j._


object AggregateByKey {
  
  def myfunc(index: Int, iter: Iterator[(String, Int)]): Iterator[String] = {
    
    iter.toList.map(x => "[Part ID: " + index + ", val: " + x + "]").iterator
    
  }
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val conf = new SparkConf().setAppName("AggregateByKey").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    val pairRDD = sc.parallelize(List(("cat", 2), ("cat", 5), ("mouse", 4), ("cat", 12), ("dog", 12), ("mouse", 2)), 2)
    
    pairRDD.mapPartitionsWithIndex(myfunc).collect.foreach(println)
    
    println("*********************************************************")
     
    pairRDD.aggregateByKey(0)(math.max(_, _), _ + _).collect.foreach(println)
    
    println("*********************************************************")
    
    pairRDD.aggregateByKey(100)(math.max(_, _), _ + _).collect.foreach(println)
    
    println("*********************************************************")
    
    // Another pairRDD example
    
    val keysWithValuesList = Array("foo=A", "foo=A", "foo=A", "foo=A", "foo=B", "bar=C", "bar=D", "bar=D")
    val data = sc.parallelize(keysWithValuesList)
    val dataPair = data.map(_.split("=")).map(x => (x(0), x(1)))
    dataPair.foreach(println)
    
    // AggregateByKey example #1
    
   // val initialSet = mutable.HashSet.empty[String]
   // val addToSet = (s: mutable.HashSet[String], v: String) => s += v
   // val mergePartitionSets = (p1: mutable.HashSet[String], p2: mutable.HashSet[String]) => p1 ++= p2
    
   // val uniqueByKey = dataPair.aggregateByKey(initialSet)(addToSet, mergePartitionSets)
    
    // AggregateByKey example #2
    
    val initialCount = 0;
    val addToCounts = (n: Int, v: String) => n + 1
    val sumPartitionCounts = (p1: Int, p2: Int) => p1 + p2
    
    val countByKey = dataPair.aggregateByKey(initialCount)(addToCounts, sumPartitionCounts)
    countByKey.foreach(println)
  
  }
  
}
