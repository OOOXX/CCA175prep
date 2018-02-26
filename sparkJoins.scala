package com.oooxxx.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD


object SparkJoins {
  
  def main(args: Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
    .appName("SparkJoins")
    .master("local")
    .config("spark.some.config.option", "spark-value")
    .getOrCreate()
    
    val emp = spark.sparkContext.parallelize(Seq((1, "revanth", 10), (2, "dravid", 20), (3, "kiran", 30), (4, "nanda", 35), (5, "kishore", 30)))
    val dep = spark.sparkContext.parallelize(Seq(("hadoop", 10), ("spark", 20), ("hive", 30), ("sqoop", 40)))
    
    val manipulated_emp = emp.keyBy(x => x._3)
    val manipulated_dep = dep.keyBy(x => x._2)
    manipulated_emp.foreach(println)
    manipulated_dep.foreach(println)
    
    val joinData = manipulated_emp.join(manipulated_dep)
    joinData.foreach(println)
    println("*****************************************")
    
    val leftOuterJoinData = manipulated_emp.leftOuterJoin(manipulated_dep)
    leftOuterJoinData.foreach(println)
    println("*****************************************")
    
    val rightOuterJoinData = manipulated_emp.rightOuterJoin(manipulated_dep)
    rightOuterJoinData.foreach(println)
    println("*****************************************")
    
    val fullOuterJoinData = manipulated_emp.fullOuterJoin(manipulated_dep)
    fullOuterJoinData.foreach(println)
    println("*****************************************")
    
    val cleaned_joined_data = joinData.map(t => (t._2._1._1, t._2._1._2, t._1, t._2._2._1))
    cleaned_joined_data.foreach(println)
    
    
  }
  
}
