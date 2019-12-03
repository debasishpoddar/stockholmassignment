package com.stklm.core.processor

import com.stklm.core.infra.InfraUtility
import com.stklm.core.records.PresureData
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ SparkSession, Dataset, DataFrame, Row }
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql._

import org.slf4j.LoggerFactory

object PressureDataProcessor {

  def main(args: Array[String]) {

    val loggermain = LoggerFactory.getLogger(classOf[PressureDataProcessor])
    
    if (args.length == 0) {
      loggermain.error("Please provide the pressure data file(s) location.")
      
      System.exit(1)
    }

    val dataProcessor = new PressureDataProcessor
    dataProcessor.processData(args(0))
    
    loggermain.info("Pressure data processed successfully")

  }
}

class PressureDataProcessor {
  
  val logger = LoggerFactory.getLogger(classOf[PressureDataProcessor])
  val appName = "Pressure Data Processor"
  val pressureData = "pressure_data"
  val infraUtility = new InfraUtility

  def processData(dataLocation: String) {

    // Create Hive enabled Session
    logger.info("Spark execution has started")
    val sparkSession = infraUtility.getHiveEnabledSparkSession(appName)

    // Read data from the HDFS location
    
    val inputData = infraUtility.readTextFile(sparkSession, dataLocation)

    import sparkSession.implicits._

    //Calling cleansingData method passing DataFrame read from hdfs location
    val cleanedData = cleansingData(inputData)

    //Insert Cleaned Dataset into hive
    insertData(cleanedData)
    logger.info("Spark execution has completed")
  

  }

  def cleansingData(inputData: DataFrame): Dataset[PresureData] = {

    // Data cleansing 
    //TODO more data cleansing can be added here
    val sparkSession = infraUtility.getHiveEnabledSparkSession(appName)
    import sparkSession.implicits._
    var splitData=sparkSession.emptyDataset[PresureData]
    logger.info("Data cleaning started")
    
    splitData = inputData
      .map { case Row(s: String) => s.split("\\s+") }
      .map(x => PresureData(x(0).toInt, x(1).toInt,
        x(2).toInt,
        if (x(3) == "NaN") BigDecimal(0) else BigDecimal(x(3)),
        if (x(4) == "NaN") BigDecimal(0) else BigDecimal(x(4)),
        if (x(5) == "NaN") BigDecimal(0) else BigDecimal(x(5))))
    
    logger.info("Data cleaned successfully")
    
    splitData

  }

  def insertData(dataFile: Dataset[PresureData]) {

    // Create Hive enabled Session
    val sparkSession = infraUtility.getHiveEnabledSparkSession(appName)
    import sparkSession.implicits._
    
    dataFile.createOrReplaceTempView(pressureData)

    try {
      sparkSession.sql(s"""
         |Insert into table STKLM_PRES_DATA
         | select year , 
         | month, 
         | day, 
         | pMorn, 
         | pNoon, 
         | pEvn
         | from pressure_data         
         """.stripMargin)
         
    logger.info("Data inserted successfully")
    } catch {
      case ex: Exception => {
        logger.error("Data insertion failed")
      }
    }
   
  }

}