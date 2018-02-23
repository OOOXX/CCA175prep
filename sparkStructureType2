package com.oooxxx.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._

// parquet agg doesnot work!!

object Spark_StructType2 {
  
  def main(args: Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
    .master("local")
    .appName("Spark_StructType2")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
    
    import spark.implicits._
    
    val personRDD = spark.sparkContext.textFile("/home/lei/Input/Data/person.txt")
    val schema = StructType(Array(StructField("FirstName", StringType, true), StructField("LastName", StringType, true), 
        StructField("Age", IntegerType, true)));
    
    val personRDDRow = personRDD.map(_.split(",")).map(p => org.apache.spark.sql.Row(p(0), p(1), p(2).toInt))
    val personDF = spark.createDataFrame(personRDDRow, schema)
    personDF.createOrReplaceTempView("person")
    
    val results = spark.sql("SELECT FirstName, Age FROM person")
    results.show()
    
    personDF.write.parquet("/home/lei/Input/Data/person_2.parquet")
    
    val personParquetDF = spark.read.parquet("/home/lei/Input/Data/person_2.parquet")
    personParquetDF.filter("Age > 25").show()
    
   // val temp = personParquetDF.groupBy("FirstName", "LastName").agg(personParquetDF.col("Age"))
   // temp.show()

  }
  
}
