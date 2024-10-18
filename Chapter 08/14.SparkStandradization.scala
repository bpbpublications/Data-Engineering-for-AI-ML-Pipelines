import org.apache.spark.ml.feature.StandardScaler

val scaler = new StandardScaler()
	.setInputCol("features")
	.setOutputCol("scaledFeatures")
	.setWithStd(true)
	.setWithMean(true)

val scalerModel = scaler.fit(dataFrame)
val standardizedData = scalerModel.transform(dataFrame)
standardizedData.show()
