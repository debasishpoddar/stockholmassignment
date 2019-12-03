# stockholmassignment
Temperature &amp; Barometer Data Processing


stk-evndata-processor : It is a spark job to process Temperature & Pressure Data through Scala and insert the same into Hive.

See the source code and internal documentation for further detail in the respective scala files:

Data Processing & Db Insertion Files :
/src/main/scala/com/stklm/core/processor/TemparatureDataProcessor.scala
/src/main/scala/com/stklm/core/processor/PressureDataProcessor.scala

Environment Setup :
/src/main/scala/com/stklm/core/processor/InfraUtility.scala

Temperature & Pressure Record Files :
/src/main/scala/com/stklm/core/processor/TemparatureData.scala
/src/main/scala/com/stklm/core/processor/PresureData

Test Cases :
/src/test/scala/com/stklm/core/processor/PressureDataProcessor.scala
/src/test/scala/com/stklm/core/processor/TemparatureDataProcessor.scala

Resources :
/src/test/resources/stockholmA_barometer_2013_2017.txt
/src/test/resources/stockholmA_daily_temp_obs_2013_2017_t1t2t3txtntm.txt

Requirements :

1)Scala 
2)Spark 
3)Eclipse IDE 2017-18 with maven build enabled.
4)Java JDK 1.8
5)Should have JAVA_HOME set for the user in Environment variable.
6)Replace the JAR_LOCATION and HDFS_DATA_LOCATION in tempdataprocessrun.sh and pressuredataprocessrun.sh with actual paths.
7)Update value of spark_warehouse_dir in InfraUtility.scala according to the environment
8)Need to configure log4j.properties in spark conf folder and will require log4j jars in every node for achieving the same.


Do you have other use cases or questions ?
Contact me at debasishpoddar10@gmail.com
