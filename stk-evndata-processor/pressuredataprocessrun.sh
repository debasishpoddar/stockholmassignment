./bin/spark-submit --class com.stklm.core.processor.TemparatureDataProcessor \
    --master yarn \
    --num-executors 1 \
    --driver-memory 1GB \
    --executor-memory 2GB \
    --executor-cores 2 \
     {JAR_LOCATION}/stk-evndata-processor-0.0.1-SNAPSHOT.jar \
    {HDFS_DATA_LOCATION}/stklm_pressure/