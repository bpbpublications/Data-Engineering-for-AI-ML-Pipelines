import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.stat.Correlation

val assembler = new VectorAssembler()
	.setInputCols(Array("feature1", "feature2", "feature3"))
	.setOutputCol("assembledFeatures")


val assembledData = assembler.transform(dataFrame)

val correlationMatrix = Correlation.corr(assembledData, "assembledFeatures").head

println(s"Correlation matrix:\n $correlationMatrix")
