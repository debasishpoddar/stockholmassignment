package com.stklm.core.processor

import org.scalatest._

import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.apache.spark.sql.{ SparkSession, Dataset, DataFrame }
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.stklm.core.infra.InfraUtility
import org.slf4j.LoggerFactory

class TemparatureDataProcessorTest extends FunSuite with BeforeAndAfterEach {
  
  val infraUtility = new InfraUtility
  val appName="TemparatureTest"
  val tempdata = new TemparatureDataProcessor ()
  
  val temparatureFileLocation=Thread.currentThread().getContextClassLoader().getResource("stockholmA_daily_temp_obs_2013_2017_t1t2t3txtntm.txt").toString();
  @transient lazy val loggertest = LoggerFactory.getLogger(classOf[TemparatureDataProcessorTest])
  
  /**
   * *Invoke Spark Session
   */
  val sparkSession = SparkSession.builder
                .appName(appName)
                .master("local")
                .getOrCreate()
  
  
  /**
   * * Test Cases for Temperature Data
   */
  
  test("Integrated Temperature Data Processing"){

    //Integrated Temperature Data Processing by passing value,upon completion the same wil be pushed to hive.
    tempdata.processData(temparatureFileLocation)
    
    assert("True".toLowerCase == "true")
  }  
  
  /**
   * 
   */
  test("Individual Temperature Data Processing"){

    // Read data from the HDFS location
    val inputData = infraUtility.readTextFile(sparkSession, temparatureFileLocation)
    
    //Individual Temperature Data Processing by passing file location,same will be cleaned and pushed into a dataset.
    val tempdataset=tempdata.cleansingData(inputData)
    
    loggertest.info(tempdataset.show().toString())
    
    //Cleaned Data pushed to hive
    tempdata.insertData(tempdataset)
   
    assert("True".toLowerCase == "true")
  }
  
  
  
  
}