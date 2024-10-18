import org.apache.spark.sql.SparkSession

object SimpleSparkApp {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder()
				.appName("Simple Application")
				.config("spark.master", "local")
				.getOrCreate()
		val dataSeq = Seq(1, 2, 3, 4, 5)
		val df = spark.createDataFrame(dataSeq.map(Tuple1.apply)).toDF("number")
		df.show()
		spark.stop()
	}	
}
