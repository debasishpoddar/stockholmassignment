package com.stklm.core.processor

import com.stklm.core.infra.InfraUtility
import com.stklm.core.records.TemparatureData
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ SparkSession, Dataset, DataFrame,Row }
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql._
import org.slf4j.LoggerFactory

object TemparatureDataProcessor {  

  def main(args: Array[String]) {

    @transient lazy val loggermain = LoggerFactory.getLogger(classOf[TemparatureDataProcessor])
    
    if (args.length == 0) {
      loggermain.error("Please provide the temparature data file(s) location.")
      System.exit(1)
    }

    val dataProcessor = new TemparatureDataProcessor
    dataProcessor.processData(args(0))
    
    loggermain.info("Temperature data processed successfully")
  }
}


class TemparatureDataProcessor {
  
  @transient lazy val logger = LoggerFactory.getLogger(classOf[TemparatureDataProcessor])
  val appName ="Temparature Data Processor"  
  val temparatureData = "temparature_data"
  val infraUtility = new InfraUtility
  
  def processData(dataLocation:String) {
    
    // Create Hive enabled Session
    logger.info("Spark execution has started")
    val sparkSession = infraUtility.getHiveEnabledSparkSession(appName)
    
    // Read data from the HDFS location
    val inputData = infraUtility.readTextFile(sparkSession, dataLocation)
    
    import sparkSession.implicits._
    
    
    //Calling cleansingData method passing DataFrame read from hdfs location
    val cleanedData=cleansingData(inputData)
    
    //Insert Cleaned Dataset into hive
    insertData(cleanedData)
    logger.info("Spark execution has completed")
    
  }
  
   
  def cleansingData(inputData:DataFrame) :Dataset[TemparatureData]= {
    
   
    // Data cleansing and data creation
    //TODO more data cleansing can be added here
    val sparkSession = infraUtility.getHiveEnabledSparkSession(appName)
    import sparkSession.implicits._
    var splitData=sparkSession.emptyDataset[TemparatureData]
    logger.info("Data Cleaning started")
    
    splitData = inputData
                   .map{case Row(s: String) => s.split("\\s+")}
                   .map(x => TemparatureData(x(0).toInt, x(1).toInt,
                              x(2).toInt,
                              if(x(3)=="NaN") BigDecimal(0) else BigDecimal(x(3)),
                              if(x(4)=="NaN") BigDecimal(0) else BigDecimal(x(4)),
                              if(x(5)=="NaN") BigDecimal(0) else BigDecimal(x(5)),
                              if(x(6)=="NaN") BigDecimal(0) else BigDecimal(x(6)),
                              if(x(7)=="NaN") BigDecimal(0) else BigDecimal(x(7)),
                              if(x(8)=="NaN") BigDecimal(0) else BigDecimal(x(8))))

    logger.info("Data cleaned successfully")                          
                              
    splitData                     
    
  }
  
  
  def insertData(dataFile:Dataset[TemparatureData]){
         
    // Create Hive enabled Session
    val sparkSession = infraUtility.getHiveEnabledSparkSession(appName)
                          
    dataFile.createOrReplaceTempView(temparatureData) 
       
    //TODO Query should not be hard coded inside the code
       
    try{
       sparkSession.sql(s"""
                       |Insert into table STKLM_TEMP_DATA
                       | select year , 
                       | month,
                       | day, 
                       | tMorn, 
                       | tNoon, 
                       | tEvn, 
                       | tmax, 
                       | tmin, 
                       | tmean  
                       | from temparature_data         
                       """.stripMargin)
     logger.info("Data inserted successfully")
       }
    catch{
         case ex: Exception => {
         logger.error("Data insertion failed")
         }
         
         }
                           
   }

}