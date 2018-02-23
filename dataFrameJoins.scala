package com.oooxxx.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFrame_Joins {
  
  case class Employee(empId: Int, empName: String, deptId: Int, salary: Int, location: String)
  case class Dept(deptId: Int, deptName: String, location: String)
  case class FinalResult(empId: Int, location: String, deptId: Int, emptName: String, salary: Int, deptName: String)
  
  
  def main(args: Array[String]){
    
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
    .master("local")
    .appName("DataFrame_Joins")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
    
    import spark.implicits._
    
    val emp = spark.sparkContext.parallelize(Seq(
      Employee(1, "Revanth", 1, 100, "BLR"),
      Employee(2, "Shyam", 1, 200, "BLR"),
      Employee(3, "Ravi", 2, 300, "AP"),
      Employee(4, "Ganesh", 2, 400, "AP"),
      Employee(5, "Revanth Reddy", 1, 9000, "BLR"),
      Employee(6, "Hari", 2, 500, "BLR"),
      Employee(7, "Hari Prasad", 2, 5500, "BLR")));
    val empDF = emp.toDF()
    empDF.show()
    
    val dept = spark.sparkContext.parallelize(Seq(
      Dept(1, "IT", "BLR"),
      Dept(2, "FINANCE", "BLR"),
      Dept(3, "SALES", "AP")));
    val deptDF = dept.toDF()
    deptDF.show()
    
    val resDF = empDF.join(deptDF, (empDF("deptID") === deptDF("deptID")) && (empDF("location") === deptDF("location")))
    
    resDF.show()
    
    val resDF2 = empDF.join(deptDF, Seq(("deptID"), ("location")))
    resDF2.show()
    
    resDF.printSchema()
    
   // val finalDF = resDF.map(x => FinalResult(x.getInt(0), x.getString(1), x.getInt(2), x.getString(3), x.getInt(4), x.getString(5))).toDF()
   // finalDF.show()
    
    empDF.filter(col("empName").like("%Revanth%")).show()
    
    empDF.createOrReplaceTempView("emp")
    
    val result = spark.sql("SELECT * FROM emp WHERE empName LIKE '%Hari'")
    result.show()
    
    empDF.sort($"salary".asc).show()
    empDF.sort($"salary".desc).show()
    empDF.orderBy("empName").show()
    empDF.orderBy($"empName".desc).show()
    empDF.drop("empName").show()
  
  }
  
}
