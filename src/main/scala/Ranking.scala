import org.apache.spark.rdd.RDD
class Ranking {

    def normalizeRDD(VectorRepresentation: VectorRepresentation): Unit = {
        println(">> normalizeRDD from Ranking.scala");
        // From mean score for each user from RDD VectorRepresentation.getUserList() RDD[(Int, Double)], update the
        // RDD VectorRepresentation.getRdd() with normalized rating values.
        val userMeanScore = VectorRepresentation.getUserList().get
        println(">> userMeanScore: " + userMeanScore)
        val userAnimeRatings = VectorRepresentation.getRdd().get
        println(">> userAnimeRatings: " + userAnimeRatings.collect().mkString(", "))
        // for each user in userAnimeRatings, update the rating value with the normalized value (rating - meanScore specific for that user)
        userAnimeRatings.mapValues( (anime) => {
            println(">> anime: " + anime)
            anime.mapValues( (rating) => {
                println(">> rating: " + rating)
                rating - userMeanScore(rating)
            })
        })
        println(">> userAnimeRatings: " + userAnimeRatings.collect().mkString(", "))
    }

}
