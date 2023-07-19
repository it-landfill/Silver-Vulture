import org.apache.spark.sql.SparkSession

object Main {

	/**
	 * Args:
	 * - args(0): booleano che indica se esecuzione in locale (default: false)
	 * - args(1): booleano che indica se rigenerare i file di ranking e vector representation (default: false)
	 * - args(2): booleano che indica se eseguire l'evaluation (default: false)
	 * - args(3): nome del bucket contenente i file (necessario solo se non in locale) (default: "")
	 * - args(4): id dell'utente per cui si vuole fare la raccomandazione (default: -1)
	 * - args(5): soglia di similarità (default: 6.0)
	 * - args(6): numero di raccomandazioni da fare (default: 10)
	 */
    def main(args: Array[String]) = {

		// Print args
		println("Args: " + args.mkString(", "))

		val localenv = if (args.length > 0) (args(0) == "true" || args(0) == "nostop") else false
		val similarityGenerator = if (args.length > 1) args(1) == "true" else false
        val similarityEvaluation = if (args.length > 2) args(2) == "true" else false
		val bucketName = if (args.length > 3) args(3) else ""
		val userID = if (args.length > 4) args(4).toInt else -1
		val threshold = if (args.length > 5) args(5).toFloat else 6.0f
		val numRecommendations = if (args.length > 6) args(6).toInt else 10

		if (!localenv && bucketName == "") {
			throw new Exception("bucketName not set")
		}

        // Generate spark session
        val sparkSession  = {		
			if (localenv) {
				SparkSession
					.builder()
					.appName("Silver-Vulture")
					.config("spark.master", "local[*]")
					.config("spark.deploy.mode", "cluster")
					.config("spark.hadoop.validateOutputSpecs", "false")
					.getOrCreate()
			} else {
				SparkSession
					.builder()
					.appName("Silver-Vulture")
					.config("spark.hadoop.validateOutputSpecs", "false")
					.getOrCreate()
			}
		}

        // Set log level (Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN)
        sparkSession.sparkContext.setLogLevel("WARN");

		val raw_path = (if (localenv) "" else ("gs://"+bucketName+"/")) + "data/rating_complete_new.csv"
		//val raw_path = (if (localenv) "" else ("gs://"+bucketName+"/")) + "data/rating_sample_example.csv"
		val out_path = (if (localenv) "" else ("gs://"+bucketName+"/")) + "out/" + userID + "/"

        val customRecommendation = new CustomRecommendation(sparkSession, localenv, bucketName)
		
		if (similarityGenerator) customRecommendation.generate(raw_path)
		else customRecommendation.load()

		if (userID > 0) {
			val recommendation = customRecommendation.recommend(userID, threshold, numRecommendations)
			DataLoader.saveCSV(Option(recommendation.repartition(1)), out_path)
		}

        if (localenv && args(0) != "nostop") {
            println("Press enter to close")
            System.in.read
        }
        sparkSession.stop()
    }
}
