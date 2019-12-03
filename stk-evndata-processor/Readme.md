stk-evndata-processor

It is an spark job to process Temperature & Pressure Data through Scala and insert the same into Hive.

See the source code and internal documentation for further detail in the respective scala files:

Data Processing & Db Insertion Files :
/src/main/scala/com/stklm/core/processor/TemparatureDataProcessor.scala
/src/main/scala/com/stklm/core/processor/PressureDataProcessor.scala

Environment Setup :
/src/main/scala/com/stklm/core/processor/InfraUtility.scala

Temperature & Pressure Record Files :
/src/main/scala/com/stklm/core/processor/TemparatureData.scala
/src/main/scala/com/stklm/core/processor/PresureData


Requirements :

Scala 
Spark 
Eclipse IDE 2017-18 with maven build enabled.
Java JDK 8
Should have JAVA_HOME set for the user in Environment variable.
Replace the JAR_LOCATION and HDFS_DATA_LOCATION in tempdataprocessrun.sh and pressuredataprocessrun.sh with actual paths.
Update value of spark_warehouse_dir in InfraUtility.scala according to the environment


Do you have other use cases or questions ?
Contact me at debasishpoddar10@gmail.com