package com.stklm.core.processor

import org.scalatest._

import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.apache.spark.sql.{ SparkSession, Dataset, DataFrame }
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.stklm.core.infra.InfraUtility
import org.apache.spark.sql.{ SparkSession, Dataset, DataFrame }
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

class PressureDataProcessorTest extends FunSuite with BeforeAndAfterEach {
  
  val infraUtility = new InfraUtility
  val appName="PressureTest"
  val pressdata = new PressureDataProcessor ()
  
  val pressureFileLocation =Thread.currentThread().getContextClassLoader().getResource("stockholmA_barometer_2013_2017.txt").toString();
  val loggertest = LoggerFactory.getLogger(classOf[PressureDataProcessorTest])
  
  /**
   * *Invoke Spark Session
   */
  val sparkSession = SparkSession.builder
                .appName(appName)
                .master("local")
                .getOrCreate()
  
  
 /**
   * * Test Cases for Pressure Data
   */
  
  test("Integrated Pressure Data Processing"){

    //Integrated Pressure Data Processing by passing value,upon completion the same wil be pushed to hive.
    pressdata.processData(pressureFileLocation)
    
    assert("True".toLowerCase == "true")
  }  
  
 
  test("Individual Pressure Data Processing"){

    // Read data from the HDFS location
    val inputData = infraUtility.readTextFile(sparkSession, pressureFileLocation)
    
    //Individual Pressure Data Processing by passing file location,same will be cleaned and pushed into a dataset.
    val pressdataset=pressdata.cleansingData(inputData)
    
    loggertest.info(pressdataset.show().toString())
    
    //Cleaned Data pushed to hive
    pressdata.insertData(pressdataset)
    
    assert("True".toLowerCase == "true")
  }
  
}