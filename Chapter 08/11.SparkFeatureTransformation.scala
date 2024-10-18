import org.apache.spark.ml.feature.PolynomialExpansion

val polyExpansion = new PolynomialExpansion()
	.setInputCol("features")
	.setOutputCol("polyFeatures")
	.setDegree(3)

val polyData = polyExpansion.transform(dataFrame)
