/Users/rhondusmithwick/Documents/SparkScalaSource/spark-1.6.0-bin-hadoop2.6/bin/spark-submit \
--class Programs.WhiteHouseSparkSQL \
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=/Users/rhondusmithwick/Documents/SparkScala/eventLogging \
/Users/rhondusmithwick/Documents/SparkScala/target/scala-2.10/sparkscala_2.10-1.0.jar; 
