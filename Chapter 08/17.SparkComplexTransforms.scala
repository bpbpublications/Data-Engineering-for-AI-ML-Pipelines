// Example for text data transformation
import org.apache.spark.ml.feature.{Tokenizer, StopWordsRemover}

val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")

val tokenized = tokenizer.transform(dataFrame)
val remover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered")

val filteredData = remover.transform(tokenized)

filteredData.show()
