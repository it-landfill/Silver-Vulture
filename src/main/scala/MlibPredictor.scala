
import org.apache.spark.rdd.{RDD}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable

class MLLibPrediction(sparkSession: SparkSession, localenv: Boolean, bucketName: String) {
	var training: RDD[Rating] = _
	var validation: RDD[Rating] = _
	var test: RDD[Rating] = _
	var bestModel: Option[MatrixFactorizationModel] = None
	var testRmse: Double = 0.0
	val modelPath = (if (localenv) "" else ("gs://"+bucketName+"/")) + "MLLib/bestModel"
  	val vectorRepr = new VectorRepresentation(sparkSession, localenv, bucketName)
	vectorRepr.load("data/")

	/** Compute RMSE (Root Mean Squared Error). */
    def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long): Double = {
        val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
        val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
            .join(data.map(x => ((x.user, x.product), x.rating)))
            .values
        math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
    }

	def generateModel(data_path: String, trainPercent: Double = 0.6, validationPercent: Double = 0.5, numPartitions: Int = 4) {
		import sparkSession.implicits._      

		val mainDF = vectorRepr  
		
		val trainingDF = mainDF.sample(false, trainPercent).repartition(numPartitions).cache()
		val evalDF = mainDF.except(trainingDF)
		val validationDF = evalDF.sample(false, validationPercent).repartition(numPartitions).cache()
		val testDF = evalDF.except(validationDF).repartition(numPartitions).cache()

		training = trainingDF.rdd
		validation = validationDF.rdd
		test = testDF.rdd


		val numTraining = training.count()
        val numValidation = validation.count()
        val numTest = test.count()
		// train models and evaluate them on the validation set

        val ranks = List(8, 12)
        val lambdas = List(0.1, 10.0)
        val numIters = List(10, 20)
        var bestValidationRmse = Double.MaxValue
        var bestRank = 0
        var bestLambda = -1.0
        var bestNumIter = -1
        for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
            val model = ALS.train(training, rank, numIter, lambda)
            val validationRmse = computeRmse(model, validation, numValidation)
            println("RMSE (validation) = " + validationRmse + " for the model trained with rank = "
                + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".")
            if (validationRmse < bestValidationRmse) {
                bestModel = Some(model)
                bestValidationRmse = validationRmse
                bestRank = rank
                bestLambda = lambda
                bestNumIter = numIter
            }
        }

		testRmse = computeRmse(bestModel.get, test, numTest)
		

        println("The best model was trained with rank = " + bestRank + " and lambda = " + bestLambda
            + ", and numIter = " + bestNumIter + ", and its RMSE on the test set is " + testRmse + ".")
	}

  def evaluateModel {
		// create a naive baseline and compare it with the best model

        val meanRating = training.union(validation).map(_.rating).mean
        val baselineRmse =
            math.sqrt(test.map(x => (meanRating - x.rating) * (meanRating - x.rating)).mean)
        val improvement = (baselineRmse - testRmse) / baselineRmse * 100
        println("The best model improves the baseline by " + "%1.2f".format(improvement) + "%.")
  }

  def predictSelected(user_id: Int, anime_to_eval: org.apache.spark.rdd.RDD[Int]): Array[Rating] = {
		bestModel.get
			.predict(anime_to_eval.map((6, _)))
			.collect()
			.sortBy(- _.rating)
			.take(50)
  }

  def predict(user_id: Int, limit: Int = 10): Array[org.apache.spark.mllib.recommendation.Rating] = {
		 import sparkSession.implicits._

		// List of anime ids that user has reviewed
		val anime_with_score = vectorRepr
		.getMainDF
		.filter(row => row.getInt(0) == user_id)
		.map(row => row.getInt(1))
		.distinct()

		// List of anime ids that user has never reviewed
		val anime_to_eval = vectorRepr
		.getMainDF
		.map(row => row.getInt(1))
		.distinct()
		.except(anime_with_score)
		
		predictSelected(user_id, anime_to_eval.rdd).take(limit)
  }

  def saveModel {
    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    val outPutPath = new Path(modelPath)

    if (fs.exists(outPutPath))
      fs.delete(outPutPath, true)
      bestModel.get.save(sparkSession.sparkContext, modelPath)
  }

  def loadModel {
    bestModel = Some(MatrixFactorizationModel.load(sparkSession.sparkContext, modelPath))
  }
}
