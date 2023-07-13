import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class CustomRecommendation(sparkSession: SparkSession) {
	val vectorRepr = new VectorRepresentation(sparkSession)
	val ranking = new Ranking(sparkSession, vectorRepr)
	val predictor = new Prediction(sparkSession, vectorRepr, ranking)

	def generate(data_path: String) {
		val mainSchema = new StructType()
			.add(StructField("user_id", IntegerType, nullable = false))
			.add(StructField("anime_id", IntegerType, nullable = false))
			.add(StructField("rating", IntegerType, nullable = false))

		val rating_complete =  DataLoader.loadCSV(sparkSession, data_path, mainSchema)

		vectorRepr.parseDF(rating_complete)
		vectorRepr.save()

		ranking.normalizeRDD()
		ranking.save()
	}

	def load() {
		vectorRepr.load()
		vectorRepr.show()
		ranking.load()
		ranking.show()
	}

	def recommend(user_id: Int, threshold: Float = 6, limit: Int = 10) {
		println("Prediction for user " + user_id + ":")
		predictor.predict(user_id, threshold, limit).show()
	}
}