package com.oooxxx.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.Text
import org.apache.log4j._

object SequenceFiles {
  
  def main(args: Array[String]) {
    
    // !problem on how to load sequenceFile??
  
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("SequenceFiles").setMaster("local[*]")
    val sc = new SparkContext(conf)
  
    val data = sc.textFile("/home/lei/Input/Data/olympics_data.txt")
    data.take(5).foreach(println)
    data.map(x => x.split(",")).map(x => (x(1).toString(), x(2).toString())).foreach(println)
    val pairs: RDD[(String, String)] = data.map(x => x.split(",")).map(x => (x(1).toString(), x(2).toString))
    pairs.saveAsSequenceFile("/home/lei/Input/Data/rdd_to_seq")
    
  //  val data1: RDD[(String, String)] = sc.sequenceFile("/home/lei/Input/Data/rdd_to_seq/part-00000", 
  //      classOf[Text], classOf[Text])
  //  data1.take(5).foreach(println)
     
    
  
  }
  
}
