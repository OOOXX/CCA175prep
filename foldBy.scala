package com.oooxxx.spark

import org.apache.spark._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j._

object FoldByKey {
  
  def main(args: Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val conf = new SparkConf().setAppName("FoldByKey").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    val employeeData = List(("Lei", 1000.0), ("Shell", 2000.0), ("Sophie", 3000.0))
    val employeeRDD = sc.parallelize(employeeData)
    
    val dummyEmployee = ("dummy", 0.0)
 
    val maxSalaryEmployee = employeeRDD.fold(dummyEmployee)((acc, employee) => {
      if (acc._2 < employee._2) employee else acc
      })
    
    println("Employee with the max salary is " + maxSalaryEmployee)
    println("---------")
    
    val deptEmployees = List(
        ("cs", ("Jack", 1000.0)),
        ("cs", ("Bron", 2000.0)),
        ("phy", ("Sam", 3000.0)),
        ("phy", ("Kim", 500.0)))
    val empRDD = sc.parallelize(deptEmployees)
    val dummyEmp = ("Dummy", 0.0)
    
    val maxSalaryEmp = empRDD.foldByKey(dummyEmp)((acc, employee) => {
      if (acc._2 < employee._2) employee else acc
    })
    
    println("Eamployee with the max salary is " + maxSalaryEmp.collect().toList)
    println("---------")
    
    val test = sc.parallelize(Array(("a", 1), ("b", 2), ("c", 1), ("a", 3), ("b", 3), ("c",6)))
    test.foldByKey(0)(_+_).collect.foreach(println)
    println("----------")
    test.foldByKey(1)(_*_).collect.foreach(println)
 
  }
}
