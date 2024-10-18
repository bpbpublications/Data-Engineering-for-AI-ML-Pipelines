import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("StructuredStreamingExample")
  .getOrCreate()

val inputStream = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribe", "topic1")
  .load()val transformedStream = inputStream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .as[(String, String)]

val query = transformedStream.writeStream
  .outputMode("append")
  .format("console")
  .start()
  
query.awaitTermination()
