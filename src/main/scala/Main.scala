import org.apache.spark.sql.SparkSession

/**
 * Variabili di ambiente disponibili:
 * - localenv: se presente, esegue in locale (se equivale a "nostop" non attende input per chiudere)
 * - sv_generation: se presente, rigenera i file di ranking e vector representation
 * - sv_valuation: se presente, esegue l'evaluation
*/
object Main {
    def main(args: Array[String]) = {

		val localenv = sys.env.contains("localenv") && sys.env("localenv") != ""
		val similarityGenerator = sys.env.contains("sv_generation") && sys.env("sv_generation") != ""
        val similarityEvaluation = sys.env.contains("sv_evaluation") && sys.env("sv_evaluation") != ""

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

		val raw_path = (if (localenv) "" else "gs://silver-vulture-data/") + "data/rating_complete_new.csv"
		//val raw_path = (if (localenv) "" else "gs://silver-vulture-data/") + "data/rating_sample_example.csv"

        val customRecommendation = new CustomRecommendation(sparkSession)
		
		if (similarityGenerator) customRecommendation.generate(raw_path)
		else customRecommendation.load()

		customRecommendation.recommend(2, 0, 10)

        if (localenv && sys.env("localenv") != "nostop") {
            println("Press enter to close")
            System.in.read
        }
        sparkSession.stop()
    }
}
