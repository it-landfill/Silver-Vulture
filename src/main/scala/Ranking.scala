import org.apache.spark.rdd.RDD

import scala.math.{pow, sqrt}
class Ranking {

    def normalizeRDD(VectorRepresentation: VectorRepresentation): Unit = {
        println(">> normalizeRDD from Ranking.scala");
        // From mean score for each user from RDD VectorRepresentation.getUserList() RDD[(Int, Double)], update the
        // RDD VectorRepresentation.getRdd() with normalized rating values.
        val userMeanScore = VectorRepresentation.getUserList().get //TODO: Gestire caso in cui sia None
        println(">> userMeanScore: " + userMeanScore)
        val userAnimeRatings = VectorRepresentation.getRdd().get //TODO: Gestire caso in cui sia None
        println(">> userAnimeRatings: " + userAnimeRatings.collect().mkString(", "))
        // for each user in userAnimeRatings, update the rating value with the normalized value (rating - meanScore specific for that user)

        val ratings = userAnimeRatings.mapValues( animeScores =>
            animeScores.map(evaluation =>
                evaluation._1 -> (evaluation._2 - userMeanScore(evaluation._1))
            )
        )


       userAnimeRatings.mapValues( (anime) => {
            anime.mapValues( (rating) => {
                rating - userMeanScore(rating)
            })
        })

        println(">> userAnimeRatings normalized: " + ratings.collect().mkString(", "))
        
        val denominator = ratings.mapValues(animeScores =>
            sqrt(animeScores.values.map(rating => pow(rating,2)).sum)
        )

        println(">> Denominator: " + denominator.collect().mkString(", "))

        //val num = ratings.reduce((a,b) => (a._1, ))

    }

}
