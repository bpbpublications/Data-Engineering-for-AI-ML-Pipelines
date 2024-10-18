import org.apache.spark.ml.feature.Bucketizer

val splits = Array(Double.NegativeInfinity, 0.5, 1.5, Double.PositiveInfinity)

val bucketizer = new Bucketizer()
	.setInputCol("feature1")
	.setOutputCol("bucketedFeatures")
	.setSplits(splits)

val bucketedData = bucketizer.transform(dataFrame)
bucketedData.show()
