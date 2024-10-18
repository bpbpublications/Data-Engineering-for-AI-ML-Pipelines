import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
.appName("Data Cleaning")
.config("spark.master", "local")
.getOrCreate()

import spark.implicits._

// Sample DataFrame with missing values

val df = Seq(
(1, "John", Some(28)),
(2, "Jane", None),
(3, "Jake", Some(42)),
(4, "Jess", None)).toDF("id", "name", "age")

// Removing rows with missing age values
val cleanDf = df.na.drop("any", Seq("age"))

cleanDf.show()


