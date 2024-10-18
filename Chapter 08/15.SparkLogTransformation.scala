import org.apache.spark.sql.functions._

val dataWithLog = dataFrame.withColumn("logFeatures", log(col("features")))

dataWithLog.show()
