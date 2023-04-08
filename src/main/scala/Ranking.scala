import org.apache.spark.rdd.RDD
class Ranking {

    def normalizeRDD(VectorRepresentation: VectorRepresentation): Unit = {
        println("normalizeRDD from Ranking.scala");
        // From mean score for each user from RDD VectorRepresentation.getUserList() RDD[(Int, Double)], update the RDD VectorRepresentation.getRdd()
        // with normalized rating values.
        // From Vector representation (AnimeID, Map(UserID, Rating)) to
        // Normalized vector representation (AnimeID, Map(UserID, NormalizedRating)
        val userScores = VectorRepresentation.getUserList().get
        //val normalizedVectorRepresentation: RDD[(Int, Map[Int, Double])] = VectorRepresentation.getRdd() match {
            //case Some(rdd) =>
                // Calculate the new Rating equal to Rating - MeanScore (given RDD VectorRepresentation.getUserList() RDD[(Int, Double)])
                //rdd.mapValues(
                //    _.map(x => (x._1, x._2 - userScores.filter(_._1 == x._1).first()._2))
               // )
            //case None      => throw new Exception("No data to print")
       // }

        //.foreach(println)
    }

    def collaborativeFilteringSimilarity(VectorRepresentation: VectorRepresentation): Unit = {
        println("collaborativeFilteringSimilarity from Ranking.scala");
        //

    }

}
