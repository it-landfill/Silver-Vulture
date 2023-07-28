
import org.apache.spark.rdd.{RDD}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

import scala.collection.mutable

class MLLibPrediction(sparkSession: SparkSession, localenv: Boolean, bucketName: String) {
	var training: RDD[Rating] = _
	var validation: RDD[Rating] = _
	var test: RDD[Rating] = _
	var bestModel: Option[MatrixFactorizationModel] = None
	var testMae: Double = 0.0
	val modelPath = (if (localenv) "" else ("gs://"+bucketName+"/")) + "MLLib/bestModel"
  	val vectorRepr = new VectorRepresentation(sparkSession, localenv, bucketName)

    def computeMae(model: MatrixFactorizationModel, data: RDD[Rating], n: Long): Double = {
		var predicitions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
		val predictionsAndRatings = predicitions.map(x => ((x.user, x.product), x.rating))
			.join(data.map(x => ((x.user, x.product), x.rating)))
			.values
		predictionsAndRatings.map(x => math.abs(x._1 - x._2)).mean
	}

	def generateModel(data_path: String, trainPercent: Double = 0.6, validationPercent: Double = 0.5, numPartitions: Int = 4) {
		import sparkSession.implicits._      

		val mainSchema = new StructType()
			.add(StructField("user_id", IntegerType, nullable = false))
			.add(StructField("anime_id", IntegerType, nullable = false))
			.add(StructField("rating", IntegerType, nullable = false))

		val rating_complete = DataLoader.loadCSV(sparkSession, data_path, mainSchema)

		vectorRepr.parseDF(rating_complete)
		vectorRepr.save("MLLib/")

		val mainDF = vectorRepr.getMainDF.map(row => Rating(row.getInt(0), row.getInt(1), row.getInt(2))).cache()
		
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
        var bestValidationMae = Double.MaxValue
        var bestRank = 0
        var bestLambda = -1.0
        var bestNumIter = -1
        for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
            val model = ALS.train(training, rank, numIter, lambda)
            val validationMae = computeMae(model, validation, numValidation)
            println("Mae (validation) = " + validationMae + " for the model trained with rank = "
                + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".")
            if (validationMae < bestValidationMae) {
                bestModel = Some(model)
                bestValidationMae = validationMae
                bestRank = rank
                bestLambda = lambda
                bestNumIter = numIter
            }
        }

		testMae = computeMae(bestModel.get, test, numTest)
		

        println("The best model was trained with rank = " + bestRank + " and lambda = " + bestLambda
            + ", and numIter = " + bestNumIter + ", and its MAE on the test set is " + testMae + ".")

		saveModel
	}

  def evaluateModel {
		// create a naive baseline and compare it with the best model
        val meanRating = training.union(validation).map(_.rating).mean
        val baselineMae = test.map(x => (meanRating - x.rating).abs).mean
        val improvement = (baselineMae - testMae) / baselineMae * 100
        println("The best model improves the baseline by " + "%1.2f".format(improvement) + "%.")
  }

  def predictSelected(user_id: Int, anime_to_eval: org.apache.spark.rdd.RDD[Int], limit: Int): DataFrame = {
		import sparkSession.implicits._
		val res = bestModel.get
					.predict(anime_to_eval.map((user_id, _)))
					.collect()
					.sortBy(- _.rating)
					.take(50)

		sparkSession.sparkContext
			.parallelize(res)
			.toDF()
			.drop("user")
			.withColumnRenamed("product", "anime_id")
			.withColumnRenamed("rating", "predicted_score")
  }

  def predict(user_id: Int, limit: Int = 10): DataFrame = {
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
		
		predictSelected(user_id, anime_to_eval.rdd, limit)
  }

	/**
	 * Save the model to the MLLib folder. This folder must not have a model saved already.
	 */
  def saveModel {
      bestModel.get.save(sparkSession.sparkContext, modelPath)
  }

  def loadModel {
    bestModel = Some(MatrixFactorizationModel.load(sparkSession.sparkContext, modelPath))
	vectorRepr.load("MLLib/")
  }
}
