import org.apache.spark.sql.SparkSession

object Main {
    def main(args: Array[String]) = {

        // Generate spark session
        val sparkSession = SparkSession
            .builder()
            .appName("Silver-Vulture")
            .config("spark.master", "local")
            .getOrCreate()

        // Load DataLoader
        val dataLoader = new DataLoader(sparkSession);
        val rating_complete =
            dataLoader.loadCSV("data/rating_sample_5.csv", true, null);
        rating_complete.show();

        val vectorRepr = new VectorRepresentation(rating_complete);
        println("Vector Representation: ")
        vectorRepr.print()

        println("Anime List: ")
        vectorRepr.getAnimeList() match {
            case Some(animeList) => animeList.foreach(println)
            case None            => println("No anime list")
        }

        println("User List: ")
        vectorRepr.getUserList() match {
            case Some(userList) => userList.foreach(println)
            case None           => println("No user list")
        }

        // Chiude la sessione Spark
        sparkSession.stop()
    }
}
