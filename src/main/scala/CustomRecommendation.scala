import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class CustomRecommendation(sparkSession: SparkSession, localenv: Boolean, bucketName: String) {
  val vectorRepr = new VectorRepresentation(sparkSession, localenv, bucketName)
  val ranking = new Ranking(sparkSession, vectorRepr, localenv, bucketName)
  val predictor = new Prediction(sparkSession, vectorRepr, ranking)

  def generate(data_path: String) {
    val mainSchema = new StructType()
      .add(StructField("user_id", IntegerType, nullable = false))
      .add(StructField("anime_id", IntegerType, nullable = false))
      .add(StructField("rating", IntegerType, nullable = false))

    val rating_complete = DataLoader.loadCSV(sparkSession, data_path, mainSchema)

    vectorRepr.parseDF(rating_complete)
    vectorRepr.save("data/")

    ranking.normalizeRDD()
    ranking.save("data/")
  }

  def load() {
    vectorRepr.load("data/")
    vectorRepr.show()
    ranking.load("data/")
    ranking.show()
  }

  def recommend(user_id: Int, threshold: Float = 6, limit: Int = 10, similarity_ceil: Double = 0.5): DataFrame = {
    println("Prediction for user " + user_id + ":")
    val pred = predictor.predict(user_id, threshold, limit, similarity_ceil)
    pred
  }
}