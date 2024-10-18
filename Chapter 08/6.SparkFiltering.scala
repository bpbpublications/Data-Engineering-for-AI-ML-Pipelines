import org.apache.spark.sql.functions._

// Sample DataFrame with potential outliers
val salesData = Seq(
	(1, 100),
	(2, 120),
	(3, 850),
	(4, 90),
	(5, 110),
	(6, 95),
	(7, 800)).toDF("id", "sales")

// Calculating quantiles
val quantiles = salesData.stat.approxQuantile("sales", Array(0.25, 0.75), 0.0)
val Q1 = quantiles(0)
val Q3 = quantiles(1)
val IQR = Q3 - Q1

// Defining bounds for outliers
val lowerRange = Q1 - 1.5 * IQR
val upperRange = Q3 + 1.5 * IQR

// Filtering out outliers
val filteredData = salesData.filter($"sales" >= lowerRange && $"sales" <= upperRange)
filteredData.show()
