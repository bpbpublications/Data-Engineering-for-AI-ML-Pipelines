import org.apache.spark.ml.feature.StandardScaler

val scaler = new StandardScaler()
	.setInputCol("features")
	.setOutputCol("scaledFeatures")
	.setWithStd(true)
	.setWithMean(false)val scalerModel = scaler.fit(dataFrame)

val scaledData = scalerModel.transform(dataFrame)
