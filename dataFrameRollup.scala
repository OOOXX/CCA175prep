package com.oooxxx.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import au.com.bytecode.opencsv.CSVParser

object DataFramesRollup {
  
  case class dataBean(week: String, campaignType: String, campaign: String, account: String, brandUnBrand: String, category: String, 
      impressions: Int, clicks: Int, cost: Double, engagements: String, patientJourney: String, device: String, indication: String, 
      country: String, region: String, metroArea: String)
  case class FinalResultRollup(week: String, campaignType: String, campaign: String, account: String, brandUnBrand: String, category: String, 
      impressions: Long, clicks: Long, cost: Double, engagements: String, patientJourney: String, device: String, indication: String, 
      country: String, region: String, metroArea: String)
  var parser = new CSVParser(',')
  
  def main(args: Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
    .master("local")
    .appName("DataFramesRollup")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
    
    import spark.implicits._
    
    val objDataBean = spark.sparkContext.textFile("/home/lei/Input/Data/campaign.csv")
    val first = objDataBean.first()
    val header: RDD[String] = spark.sparkContext.parallelize(Array(first))
    
    val dropHeaderRDD = objDataBean.mapPartitions(_.drop(1))
    val filterEmptyRowsRDD = dropHeaderRDD.map(line => isNotEmptyLine(line))
    
    val objDataBeanDF = dropHeaderRDD.map(getTokens(_))
      .map(x => dataBean(x(0), x(1), x(2), x(3), x(4), x(5), checkNullForInt(x(6)), checkNullForInt(x(7)), checkNullForDouble(x(8)), x(9), 
          x(10), x(11), x(12), x(13), x(14), x(15))).toDF()
    objDataBeanDF.show()
    
    val aggDF = objDataBeanDF.groupBy("week", "campaignType", "campaign", "account", "brandUnBrand", "category", "impressions", "clicks", "cost", "engagements", "patientJourney", "device", "indication", "country", "region", "metroArea").
      agg(sum(objDataBeanDF.col("impressions")), sum(objDataBeanDF.col("clicks")), sum(objDataBeanDF.col("cost")))
      
   aggDF.show()
    
    val finalDF = aggDF.map(x => FinalResultRollup(x.getString(0), x.getString(1), x.getString(2), x.getString(3), x.getString(4), x.getString(5), 
        x.getLong(16), x.getLong(17), x.getDouble(18), x.getString(9), x.getString(10), x.getString(11), x.getString(12), x.getString(13), 
        x.getString(14), x.getString(15))).toDF()
        
    finalDF.show()
    
    val finalRDD = finalDF.rdd.map(row => checkForComma(row(0)) + "," + checkForComma(row(1)) + "," + checkForComma(row(2)) + ","
        + checkForComma(row(3)) + "," + checkForComma(row(4)) + "," + checkForComma(row(5)) + "," + row(6) + "," + row(7) + "," + row(8)
        + "," + checkForComma(row(9)) + "," + checkForComma(row(10)) + "," + checkForComma(row(11)) + "," + checkForComma(row(12)) + ","
        + checkForComma(row(13)) + "," + checkForComma(row(14)) + "," + checkForComma(row(15)))
        
    header.union(finalRDD).coalesce(1, true).saveAsTextFile("/home/lei/Input/Data/campaign.txt")
    
    

    
    
  }
  
  private def checkNullForInt(value: String): Integer = {
    if (!"".equals(value)) {
      return value.toInt;
    }
    return 0;
  }

  private def checkNullForDouble(value: String): Double = {
    if (!"".equals(value)) {
      return value.toDouble;
    }
    return 0.0;
  }
  
  private def getTokens(value: String): Array[String] = {
    if (!"".equals(value)) {
      var tokens: Array[String] = parser.parseLine(value);
      if (tokens.length != 16) {
        println("Line = " + value + ",Token Length= " + tokens.length)
      }
      return tokens;
    }
    return null;
  }
  
  
  private def isNotEmptyLine(value: String): Boolean = {
    if ("".equals(value)) {
      return false;
    } else {
      if (value.length() > 20) {
        return true;
      } else return false;
    }
    return true;
    
  }
  
  private def checkForComma(value: Any): String = {
    if (!"".equals(value) && value.toString().indexOf(',') > -1) {
      val newvalue = "\"" + value + "\"";
      return newvalue;
    }
    return value.toString();
  }
  
}
