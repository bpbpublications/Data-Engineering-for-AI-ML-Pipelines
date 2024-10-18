import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("Logistic Regression Example").getOrCreate()

// Prepare training data from a list of (label, features) tuples.
val trainingData = spark.createDataFrame(Seq(
  (1.0, Vectors.dense(0.0, 1.1, 0.1)),(0.0, Vectors.dense(2.0, 1.0, -1.0)),
  (0.0, Vectors.dense(2.0, 1.3, 1.0)),
  (1.0, Vectors.dense(0.0, 1.2, -0.5))
  )).toDF("label", "features")

// Create a LogisticRegression instance. This instance is an Estimator.
val lr = new LogisticRegression()
  .setMaxIter(10)
  .setRegParam(0.01)

// Fit the model
val model = lr.fit(trainingData)

// Prepare test data
val testData = spark.createDataFrame(Seq(
  (1.0, Vectors.dense(-1.0, 1.5, 1.3)),
  (0.0, Vectors.dense(3.0, 2.0, -0.1)),
  (1.0, Vectors.dense(0.0, 2.2, -1.5))
  )).toDF("label", "features")

// Make predictions on test data
val predictions = model.transform(testData)// Evaluate the model
val evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("label")
  .setPredictionCol("prediction")
  .setMetricName("accuracy")

val accuracy = evaluator.evaluate(predictions)
println(s"Accuracy = $accuracy")

spark.stop()
