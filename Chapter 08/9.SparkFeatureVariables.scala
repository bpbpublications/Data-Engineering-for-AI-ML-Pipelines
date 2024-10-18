import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder}
val indexer = new StringIndexer()
	.setInputCol("categoryColumn")
	.setOutputCol("categoryIndex")
	.fit(dataFrame)

val indexed = indexer.transform(dataFrame)
val encoder = new OneHotEncoder()
	.setInputCol("categoryIndex")
	.setOutputCol("categoryVec")

val encoded = encoder.transform(indexed)
