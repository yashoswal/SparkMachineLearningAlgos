import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.util.MLUtils
import org.apache.log4j.Logger
import org.apache.log4j.Level

object SimpleApp {
  def main(args: Array[String]) {
	Logger.getLogger("org").setLevel(Level.OFF)
	Logger.getLogger("akka").setLevel(Level.OFF)
	val conf = new SparkConf().setAppName("RandomForest")
    	val sc = new SparkContext(conf)	
		// Load and parse the data file.
	val rawData = sc.textFile("/home/yash/Downloads/joined click fraud data/counts_with_time_60_without_partnerid_feb9")
	val rawData1 = sc.textFile("/home/yash/Downloads/joined click fraud data/counts_with_time_60_without_partnerid_feb23")

val trainingData = getLabeledPoints_observe_ok(rawData)
val testData = getLabeledPoints_observe_ok(rawData1)
//val splits = data.randomSplit(Array(0.7, 0.3))
//val (trainingData, testData) = (splits(0), splits(1))

// Train a RandomForest model.
/*val treeStrategy = Strategy.defaultStrategy("Classification")
val numTrees = 20 // Use more in practice.
val featureSubsetStrategy = "auto" // Let the algorithm choose.
val model = RandomForest.trainClassifier(trainingData,treeStrategy, numTrees, featureSubsetStrategy, seed = 12345)
*/

val numClasses = 2
val categoricalFeaturesInfo = Map[Int, Int]((4,10))
//val categoricalFeaturesInfo = Map[Int, Int]()
val numTrees = 30 // Use more in practice.
val featureSubsetStrategy = "auto" // Let the algorithm choose.
val impurity = "entropy"
val maxDepth = 5
val maxBins = 32

val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
  numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
// Evaluate model on test instances and compute test error
val testErr = testData.map { point =>val prediction = model.predict(point.features)
  if (point.label == prediction) 1.0 else 0.0
}.mean()
println("Test Error = " + testErr)
//println("Learned Random Forest:n" + model.toDebugString)
val metrics = getMetrics(model,testData)
println(metrics.confusionMatrix)
}

def getMetrics(model: RandomForestModel, data: RDD[LabeledPoint]):MulticlassMetrics = {
  		//val predictionsAndLabels = data.map(example =>(model.predict(example.features), example.label))
		val predictionsAndLabels = data.map { point =>val prediction = model.predict(point.features)
 					   (point.label, prediction)
					}
		//predictionsAndLabels.saveAsTextFile("/home/yash/Downloads/click-fraud-data/output")
		val testErr = predictionsAndLabels.filter(r => r._1 != r._2).count.toDouble / data.count()
		println("Test Error = " + testErr)
		new MulticlassMetrics(predictionsAndLabels)
  		//new BinaryClassificationMetrics(predictionsAndLabels)
   }
def getLabeledPoints_observe_ok(rawData:RDD[String]):RDD[LabeledPoint] = {
	rawData.map { line => val values = line.split(',').map(_.toDouble)
		if(values(7)==1.0){values(7)=0.0}else if(values(7)==2.0){values(7)=1.0}
		//if(values.last==1.0){values(8)=0.0}else if(values.last==2.0){values(8)=1.0}
  		val featureVector = Vectors.dense(values.init) 
  		val label = values.last
  		LabeledPoint(label, featureVector) }
}
}
