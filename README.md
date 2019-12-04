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
6)Need to configure log4j.properties in spark conf folder and will require log4j jars in every node for achieving the same.


Build :

To build it, need to download and unpack the latest (or recent) version of Maven (https://maven.apache.org/download.cgi) and put the mvn command on system path. Then, need to install a Java 1.8 (or higher) JDK (not JRE!), and can run java from the command line. Now run mvn clean install and Maven will compile the project, an put the results in a jar file in the target directory.

Run the code :

1) To process pressure data please run the below command there by replacing "JAR_LOCATION" with the desired path of the jar built using above build step and "HDFS_DATA_LOCATION" with the desired HDFS path of pressure data file.

./bin/spark-submit --class com.stklm.core.processor.PressureDataProcessor \
    --master yarn \
    --num-executors 1 \
    --driver-memory 1GB \
    --executor-memory 2GB \
    --executor-cores 2 \
     {JAR_LOCATION}/stk-evndata-processor-0.0.1-SNAPSHOT.jar \
    {HDFS_DATA_LOCATION}/stklm_pressure/



2) To process temperature data please run the below command there by replacing "JAR_LOCATION" with the desired path of the jar built using above build step and "HDFS_DATA_LOCATION" with the desired HDFS path of temperature data file.

./bin/spark-submit --class com.stklm.core.processor.TemparatureDataProcessor \
    --master yarn \
    --num-executors 1 \
    --driver-memory 1GB \
    --executor-memory 2GB \
    --executor-cores 2 \
     {JAR_LOCATION}/stk-evndata-processor-0.0.1-SNAPSHOT.jar \
    {HDFS_DATA_LOCATION}/stklm_temparature/


Do you have other use cases or questions ?
Contact me at debasishpoddar10@gmail.com
