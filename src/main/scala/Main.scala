import org.apache.spark.sql.SparkSession

object Main {

	/**
	 * Args:
	 * - args(0): booleano che indica se esecuzione in locale (default: false)
	 * - args(1): booleano che indica se rigenerare i file di ranking e vector representation (default: false)
	 * - args(2): booleano che indica se eseguire l'evaluation (default: false)
	 * - args(3): nome del bucket contenente i file (necessario solo se non in locale) (default: "")
	 */
    def main(args: Array[String]) = {

		// Print args
		println("Args: " + args.mkString(", "))

		val localenv = if (args.length > 0) (args(0) == "true" || args(0) == "nostop") else false
		val similarityGenerator = if (args.length > 1) args(1) == "true" else false
        val similarityEvaluation = if (args.length > 2) args(2) == "true" else false
		val bucketName = if (args.length > 3) args(3) else ""

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

		val raw_path = (if (localenv) "" else "gs://bucketName/") + "data/rating_complete_new.csv"
		//val raw_path = (if (localenv) "" else "gs://bucketName/") + "data/rating_sample_example.csv"

        val customRecommendation = new CustomRecommendation(sparkSession, localenv, bucketName)
		
		if (similarityGenerator) customRecommendation.generate(raw_path)
		else customRecommendation.load()

		customRecommendation.recommend(2, 0, 10)

        if (localenv && args(0) != "nostop") {
            println("Press enter to close")
            System.in.read
        }
        sparkSession.stop()
    }
}
