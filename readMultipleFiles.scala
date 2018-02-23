package com.oooxxx.spark

import org.apache.log4j._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ReadMultipleFiles {
  
  case class Employee(empid: Int, name: String, dept: String, salary: Int, nop: Int)
  
  def main(args: Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
    .master("local")
    .appName("ReadMultipleFiles")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

    import spark.implicits._
    
    val empDataRDD = spark.sparkContext.textFile("/home/lei/Input/Data/emp.txt").coalesce(1, false)
    val filteredRDD = empDataRDD.filter(line => !line.contains("empid"))
    val empDF = filteredRDD.filter{line => line.length() > 0}
    .map(_.split("\\|"))
    .map(p => Employee(p(0).trim.toInt, p(1), p(2), p(3).trim.toInt, p(4).trim.toInt))
    .toDF()
    
    empDF.show()
    
  }
  
}
