import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{FloatType, IntegerType, StructField, StructType}

class Evaluation (sparkSession: SparkSession, localenv: Boolean, bucketName: String){

	val vectorRepr = new VectorRepresentation(sparkSession, localenv, bucketName)
	val ranking = new Ranking(sparkSession, vectorRepr, localenv, bucketName)
	val predictor = new Prediction(sparkSession, vectorRepr, ranking)
	var evaluationDF: Option[DataFrame] = None

	val evaluationSchema = new StructType()
            .add(StructField("user_id", IntegerType, nullable = false))
            .add(StructField("anime_id", IntegerType, nullable = false))
            .add(StructField("rating", IntegerType, nullable = false))
            .add(StructField("normalized_rating", FloatType, nullable = false))

	def generateTestDF(user_id: Int, sampleRate: Double = 0.2) {
		// Define a local copy in order to modify it
		var sRate = sampleRate
		// Load the vector representation
		vectorRepr.load("data/")
		// Get the main DF
		val mainDF = vectorRepr.getMainDF
		// Filter for selected user
		val userScores = mainDF.filter(row => row.getInt(0) == user_id)
		// Take a sample of the main DF
		var evaluation = userScores.sample(false, sRate)

		// If sample is empty, increase the sample rate
		while (evaluation.isEmpty && sRate < 1) {
			sRate += 0.1
			println("Increasing sample rate to " + sRate)
			evaluation = userScores.sample(false, sRate)
		}

		if (evaluation.isEmpty) {
			throw new Exception("Unable to generate a sample for specified user")
		}

		// Remove the sample from the main DF
		val testDF = mainDF.except(evaluation)

		evaluationDF = Some(evaluation)

		// Set the main DF to the test DF
		vectorRepr.setMainDF(testDF)
		// Normalize the test DF
		ranking.normalizeRDD()

		vectorRepr.save("eval/")
		ranking.save("eval/")
		DataLoader.saveCSV(evaluationDF, "eval/evaluation")
	}

	def loadTestDF {
		vectorRepr.load("eval/")
		ranking.load("eval/")
		evaluationDF = Some(DataLoader.loadCSV(sparkSession, "eval/evaluation", evaluationSchema))
	}

	def evaluateCutomRecommendations(user_id: Int): Double = {
		 import sparkSession.implicits._

		// Load the list of anime to evaluate
		val anime_to_eval = evaluationDF.get.select("anime_id")

		// Load the test data
		val evalDF = evaluationDF.get

		// Get the predictions for the removed anime
		val predicitions = predictor.predictSelected(user_id, anime_to_eval, -1, -1)

		if (anime_to_eval.count() != predicitions.count()) {
			anime_to_eval.show()
			predicitions.show()
			throw new Exception("anime_to_eval.count() != predicitions.count()")
		}

		predicitions.join(evalDF, "anime_id")
			.map(row => (row.getDouble(1) - row.getInt(3)).abs)
			.reduce(_ + _) / predicitions.count()
	}
}