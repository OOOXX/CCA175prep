package com.oooxxx.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j._
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object Cars {
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("Cars").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    case class cars(make: String, model: String, mpg: String, cylinders: Integer, engine_disp: Integer, 
        horsepower: Integer, weight: Integer, accelerate: Double, year: Integer, origin: String)
    
    val rawData = sc.textFile("/home/OOOXX/Input/Data/cars.txt")
    rawData.take(5).foreach(println)
         
    val carsData = rawData.map(x => x.split("\t"))
      .map(x => cars(x(0).toString, x(1).toString, x(2).toString, x(3).toInt, x(4).toInt, x(5).toInt, x(6).toInt, 
          x(7).toDouble, x(8).toInt, x(9).toString));
    carsData.take(5).foreach(println)
    carsData.cache()
    
    val originWiseCount = carsData.map(x => (x.origin, 1)).reduceByKey((x, y) => x + y)
    println("originWiseCount " + originWiseCount.collect().mkString(", "))
    
    val americanCars = carsData.filter(x => x.origin == "American")
    println("American Cars Counts = " + americanCars.count)
    
   // val makeWeightSum = americanCars.map(x => (x.make, x.weight.toInt)).combineByKey((x: Int) => (x, 1),
   //     (acc: (Int, Int), x) => (acc._1 + x, acc._2 + 1),
   //     (acc1: (Int, Int), acc2: (Int, Int) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    
    val makeWeightSum = americanCars.map(x => (x.make, x.weight.toInt)).combineByKey((x: Int) => (x, 1),
      (acc: (Int, Int), x) => (acc._1 + x, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
        
    println("American Cars makeWeightSum = " + makeWeightSum.collect().mkString(","))
    
    val makeWeightAvg = makeWeightSum.map(x => (x._1, x._2._1/x._2._2))
    println("The average weight of American cars are " + makeWeightAvg.collect().mkString(","))
      
    
  }
  
}
