import org.apache.spark.sql.SparkSession

object Main {
    def main(args: Array[String]) = {
        // Generate spark session
        val sparkSession = SparkSession
            .builder()
            .appName("Silver-Vulture")
            .config("spark.master", "local[4]")
            .config("spark.deploy.mode", "cluster")
            .config("spark.driver.memory", "6G")
            .config("spark.hadoop.validateOutputSpecs", "false")
            .config("spark.executor.memory", "6G")
            .getOrCreate()

        // Set log level (Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN)
        sparkSession.sparkContext.setLogLevel("WARN");

        // Load DataLoader
        val dataLoader = new DataLoader(sparkSession);
        val rating_complete =
            dataLoader.loadCSV("data/rating_complete.csv", true, null);
        rating_complete.show();

        val vectorRepr = new VectorRepresentation(rating_complete);
        //println("Vector Representation: ")
        // vectorRepr.print()

        // TODO: Lorenzo non lasciare garbage code
        val ranking = new Ranking();
        ranking.normalizeRDD(vectorRepr);

        // println("Anime List: ")
        /* vectorRepr.getAnimeList() match {
            case Some(animeList) => animeList.foreach(println)
            case None            => println("No anime list")
        }

        println("User List: ")
        vectorRepr.getUserList() match {
            case Some(userList) => userList.foreach(println)
            case None           => println("No user list")
        } */
        //vectorRepr.saveToFile()
        //vectorRepr.loadFromFile(sparkSession:SparkSession)
        // Chiude la sessione Spark
        sparkSession.stop()
    }
}
