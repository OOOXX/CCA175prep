package com.oooxxx.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFrame {
  
  case class Employee(empid: Int, name: String, dept: String, salary: Int, nop: Int)
  case class AggregatedEmpData(empid: Int, name: String, dept: String, sumsalary: Long, sumnop: Long, maxsalary: Int)
  
  def main(args: Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
    .master("local")
    .appName("DataFrame")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
    
    import spark.implicits._
    
    val empRDD = spark.sparkContext.textFile("/home/OOOXX/Input/Data/emp.txt")
    val empDF = empRDD.mapPartitions(_.drop(1)).filter(x => x.length() > 0)
    .map(_.split('|'))
    .map(p => Employee(p(0).trim.toInt, p(1), p(2), p(3).trim.toInt, p(4).trim.toInt))
    .toDF()
    empDF.show()
    println("**********")
    
   
    
    val aggDF = empDF.groupBy("empid", "name", "dept").agg(sum(empDF.col("salary")), sum(empDF.col("nop")), max(empDF.col("salary")))
    aggDF.show()
    aggDF.printSchema()
    
    val finalDF = aggDF.map(row => AggregatedEmpData(row.getInt(0), row.getString(1), row.getString(2), row.getLong(3), 
        row.getLong(4), row.getInt(5)));
    println(finalDF.first())
    finalDF.show()
    
    println("***********")
    aggDF.rdd.coalesce(1, false).foreach(println)
    
    empDF.groupBy("empid").agg(max(empDF.col("salary"))).show()
    empDF.select(max($"salary")).show()

  }
  
}
