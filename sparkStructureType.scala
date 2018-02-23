package com.oooxxx.spark

import org.apache.log4j._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._

object Spark_StructType {
  
  // Basically, method to convert a TXT file into column/row table.
  // !problem on sqlDataFrame.map, overloaded method with alternative 
  // Solution please see Spark_StructType2.scala
  
  def main(args: Array[String]){
    
       
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
    .master("local")
    .appName("StructType")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
    
    import spark.implicits._
    
    val peopleRDD = spark.sparkContext.textFile("/home/lei/Input/Data/person.txt")
    peopleRDD.foreach(println)
    println("********************")

    val schemaString = "firstName lastName age"
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))   
    println(fields)
    println("********************")
    val schema = StructType(fields)
    println(schema)
    println("********************")
    
    val rowRDD = peopleRDD.map(_.split(",")).map(attributes => Row(attributes(0), attributes(1), attributes(2).trim))
    rowRDD.foreach(println)
    println("********************")
    val peopleDF = spark.createDataFrame(rowRDD, schema)
    peopleDF.show()
    println("********************")
    peopleDF.createOrReplaceTempView("people")
    
    val results = spark.sql("SELECT firstName,age FROM people")
    results.show()
    
    //results.map(a => a(0))
    
  }
  
}
