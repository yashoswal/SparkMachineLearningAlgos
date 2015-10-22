import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.log4j.Logger
import org.apache.log4j.Level



object SimpleApp {
  def main(args: Array[String]) {
	Logger.getLogger("org").setLevel(Level.OFF)
	Logger.getLogger("akka").setLevel(Level.OFF)
	val conf = new SparkConf().setAppName("Decision Tree")
    	val sc = new SparkContext(conf)			

	val rawData = sc.textFile("/home/yash/Downloads/joined click fraud data/counts_with_time_60_without_partnerid_feb9")
	
	val data = getLabeledPoints_observe_ok(rawData)
	
	val Array(trainData, cvData, testData) = data.randomSplit(Array(1.0, 0.0, 0.0))

	trainData.cache()
	cvData.cache()
	testData.cache()	
	
	val categoricalFeaturesInfo = Map[Int, Int]((4,10))
	//val categoricalFeaturesInfo = Map[Int, Int]((0,5710),(1,3081),(2,6109),(3,197),(4,10),(5,745088))
	//val categoricalFeaturesInfo = Map[Int, Int]((0,2859),(1,1714),(2,4144),(3,174),(4,10),(5,67261))
        val model = DecisionTree.trainClassifier(trainData, 2, categoricalFeaturesInfo, "gini", 5, 100)
 	val metrics = getMetrics(model, cvData)
	val test_metrics = getMetrics(model, testData)	
	//println("CvData precision = " + metrics.precision)	
	//	println("testData precision = " + test_metrics.precision)

	val rawData1 = sc.textFile("/home/yash/Downloads/joined click fraud data/counts_with_time_60_without_partnerid_feb23")  
	val data1 = getLabeledPoints_observe_ok(rawData1)
	val metrics1 = getMetrics(model, data1)
	//println("8 march precision = " + metrics1.precision)
	//println("8 march false positive = " + metrics1.falsePositiveRate(0))
	println(metrics1.confusionMatrix)
	//println("8 march PR = " + metrics1.areaUnderPR)
	//println("8 march ROC = " + metrics1.areaUnderROC)
	//metrics1.roc.foreach(println)
	
	}

def getMetrics(model: DecisionTreeModel, data: RDD[LabeledPoint]):MulticlassMetrics = {
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
		//if(values(6)==1.0){values(6)=0.0}else if(values(6)==2.0){values(6)=1.0}
  		val featureVector = Vectors.dense(values.init) 
  		val label = values.last
  		LabeledPoint(label, featureVector) }
}

def getLabeledPoints_observe_fraud(rawData:RDD[String]):RDD[LabeledPoint] = {
	rawData.map { line => val values = line.split(',').map(_.toDouble)
		//if(values(7)==2.0){values(7)=1.0}
		if(values(6)==2.0){values(6)=1.0}
  		val featureVector = Vectors.dense(values.init) 
  		val label = values.last
  		LabeledPoint(label, featureVector) }
}
}
