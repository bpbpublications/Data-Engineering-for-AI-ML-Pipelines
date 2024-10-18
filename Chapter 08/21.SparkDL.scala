import com.intel.analytics.bigdl.nn._
import com.intel.analytics.bigdl.utils.Engine
import com.intel.analytics.bigdl.dataset.image._

Engine.init

val model = Sequential()
  .add(Reshape(Array(3, 224, 224)))
  .add(SpatialConvolution(3, 12, 5, 5))
  .add(ReLU())
  .add(SpatialMaxPooling(2, 2, 2, 2))
  .add(SpatialConvolution(12, 20, 5, 5))
  .add(ReLU())
  .add(SpatialMaxPooling(2, 2, 2, 2))
  .add(View(20 * 53 * 53))
  .add(Linear(20 * 53 * 53, 100))
  .add(ReLU())
  .add(Linear(100, 10))
  .add(LogSoftMax())

// Assuming 'trainImages' is an RDD of labeled images

val optimizer = Optimizer(
  model = model,
  sampleRDD = trainImages,
  criterion = new ClassNLLCriterion(),
  batchSize = 64
)
optimizer.optimize()
