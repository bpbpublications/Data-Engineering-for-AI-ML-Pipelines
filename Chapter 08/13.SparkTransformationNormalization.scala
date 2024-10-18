import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.Vectors

val dataFrame = spark.createDataFrame(Seq(
	(0, Vectors.dense(1.0, 0.1, -1.0)),
	(1, Vectors.dense(2.0, 1.1, 1.0)),
	(2, Vectors.dense(3.0, 10.1, 3.0))
	)).toDF("id", "features")

val scaler = new MinMaxScaler()
	.setInputCol("features")
	.setOutputCol("scaledFeatures")

val scalerModel = scaler.fit(dataFrame)
val scaledData = scalerModel.transform(dataFrame)
scaledData.show()
