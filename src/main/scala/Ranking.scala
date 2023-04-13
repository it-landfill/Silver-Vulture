import scala.math.{pow, sqrt}
class Ranking {

    def normalizeRDD(VectorRepresentation: VectorRepresentation): Unit = {
        println(">> normalizeRDD from Ranking.scala");
        // From mean score for each user from RDD VectorRepresentation.getUserList() RDD[(Int, Double)], update the
        // RDD VectorRepresentation.getRdd() with normalized rating values.
        val userMeanScore =
            VectorRepresentation
                .getUserList()
                .get // TODO: Gestire caso in cui sia None

        val userAnimeRatings =
            VectorRepresentation
                .getRdd()
                .get // TODO: Gestire caso in cui sia None

        // for each user in userAnimeRatings, update the rating value with the normalized value (rating - meanScore specific for that user)
        val ratings = userAnimeRatings.mapValues(animeScores =>
            animeScores.map(evaluation =>
                evaluation._1 -> (evaluation._2 - userMeanScore(
                  evaluation._1
                )) // FIXME: This is a Map, it must not be used in large df (like the user one...)
            )
        )

        val cart = ratings
            .cartesian(ratings)
            .filter(pair => pair._1._1 < pair._2._1) // Handle symmetry

        // Better solution since it does not re evaluate the denominator each time but potentially worse since it uses an external Map

        val denominator = ratings
            .mapValues(animeScores =>
                sqrt(animeScores.values.map(rating => pow(rating, 2)).sum)
            )
            .collectAsMap()

        // println(">> Denominator: " + denominator)

        val numerator = cart.map(pair =>
            (
              pair._1._1,
              pair._2._1,
              pair._1._2
                  .map(animeScore1 =>
                      if (pair._2._2.contains(animeScore1._1)) {
                          animeScore1._2 * pair._2._2(animeScore1._1)
                      } else {
                          0
                      }
                  )
                  .sum / (
                denominator(pair._1._1) * denominator(pair._2._1)
              )
            )
        )

        // println(">> Numerator: " + numerator.collect().mkString(", "))
    }

}
