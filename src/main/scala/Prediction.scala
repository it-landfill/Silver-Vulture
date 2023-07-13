import org.apache.spark.sql.functions.{col, slice, struct}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable

class Prediction(session: SparkSession, vectorRepresentation: VectorRepresentation, ranking: Ranking) {
  def predict(user_id: Int, threshold: Float = 6, limit: Int = 10): DataFrame = {
    /*
    Given an userid, the function returns an anime that might be of interest

    https://medium.com/mlearning-ai/collaborative-filtering-similarity-calculations-a974ae4650

    animeScores = {}
    per ogni anime che l'utente non ha recensito {
      top = topN dell'anime
      num = 0
      den = 0
      per ogni anime in top {
        num +=  votoSimilarità di Anime in top e Anime in valutazione * voto normalizzato utente di Anime in top
      }
      per ogni anime in top che utente ha recensito {
        den +=  votoSimilarità di Anime in top e Anime in valutazione
      }
      animeScore[anime_id] = num/den + avg_user_rating
    }

    prendo da animeScore quelli con valore più alto
     */

    import session.implicits._

    // List of anime ids that user has reviewed
    val anime_with_score = vectorRepresentation
      .getMainDF
      .filter(row => row.getInt(0) == user_id)
      .map(row => row.getInt(1))
      .distinct()

    // List of anime ids that user has never reviewed
    val anime_to_eval = vectorRepresentation
      .getMainDF
      .map(row => row.getInt(1))
      .distinct()
      .except(anime_with_score)


    val similarityDF = ranking.getSimilarityDF
    val main_df = vectorRepresentation.getMainDF
    val avg_user_rating = vectorRepresentation.getUserList.filter(row => row.getInt(0) == user_id).first().getFloat(1)

    import org.apache.spark.sql.functions.collect_list

    val topN = anime_to_eval
      .as("DF1")
      .join(
        similarityDF.as("DF2"),
        col("DF1.value") === col("DF2.anime_1_id") || col("DF1.value") === col("DF2.anime_2_id"),
        "inner")
      .groupBy("value", "anime_1_id", "anime_2_id")
      .mean()
      .drop("avg(value)", "avg(anime_1_id)", "avg(anime_2_id)")
      .map(row => (
        row.getInt(0),
        if (row.getInt(0) == row.getInt(1)) row.getInt(2)
        else row.getInt(1),
        row.getDouble(3)
      ))
      .toDF("anime_1_id", "anime_2_id", "similarity")
      .as("DF1")
      .join(main_df.as("DF2"), col("DF2.user_id") === user_id && col("DF1.anime_2_id") === col("DF2.anime_id"), "leftouter")
      .drop("user_id", "anime_id", "rating")
      .na.fill(0)
      .sort(col("anime_1_id"), col("similarity").desc)
      .groupBy(col("anime_1_id"))
      .agg(slice(collect_list(struct(col("anime_2_id"), col("similarity"), col("normalized_rating"))), 1, 5).alias("topK"))


    var predictions = topN.map((row: Row) => {
      import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
      var num = 0.0
      var den = 0.0
      val topK = row.getAs[mutable.WrappedArray[GenericRowWithSchema]](1)
      topK.foreach(el => {
        val sim = el.getDouble(1).toFloat
        val rating = el.getFloat(2)
        if (rating != 0) num += sim * rating
        den += sim
      })

      (
        row.getInt(0),
        (num / den) + avg_user_rating
      )
    }
    )
      .toDF("anime_id", "predicted_score")
      .orderBy("predicted_score")

	if (limit > 0) predictions = predictions.limit(limit)
	if (threshold > 0) predictions = predictions.filter(row => row.getDouble(1) >= threshold)

    predictions
  }
}
