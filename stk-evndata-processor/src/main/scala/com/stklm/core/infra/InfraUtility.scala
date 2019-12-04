package com.stklm.core.infra

import org.apache.spark.sql.{ SparkSession, Dataset, DataFrame }
import org.apache.spark.sql.functions._

/**
 * InfraUtility class contains all the spark framework interaction methods
 */
class InfraUtility {

  val spark_warehouse_dir = "spark.sql.warehouse.dir"

 /**
  *  create Spark session without hive support
  *  @return SparkSession
  */
  
  def getSparkSession(appName: String): SparkSession = {
    SparkSession.builder
                .appName(appName)
                .master("local")
                .getOrCreate()
  }

  /**
   *  Create Hive Enabled Spark session for Spark- Hive interaction
   *  @return SparkSession
   */
  def getHiveEnabledSparkSession(appName: String): SparkSession = {
    SparkSession.builder
                .enableHiveSupport()
                .appName(appName)
                .config(spark_warehouse_dir, "/usr/manageddb/stklmdb/")
                .getOrCreate()
  }

  /**
   * Read the data from the text file based on the supplied delimeter
   */
  def readTextFile(spark: SparkSession, filePath: String): DataFrame = {
    
    val dataFrame = spark.read
                         .text(filePath)
    dataFrame
  }
}
