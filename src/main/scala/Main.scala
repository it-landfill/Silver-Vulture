import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.functions.{col}

/**
 * Variabili di ambiente disponibili:
 * - localenv: se presente, esegue in locale (se equivale a "nostop" non attende input per chiudere)
 * - sv-generation: se presente, rigenera i file di ranking e vector representation
 * - sv-evaluation: se presente, esegue l'evaluation
*/
object Main {
    def main(args: Array[String]) = {
        // Generate spark session
        val sparkSession  = {		
			if (sys.env.contains("localenv")) {
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

        val similarityGenerator = sys.env.contains("sv-generation")
        val similarityEvaluation = sys.env.contains("sv-evaluation")

        val vectorRepr = new VectorRepresentation(sparkSession)
        val ranking = new Ranking(sparkSession, vectorRepr)
        val predictor = new Prediction(sparkSession, vectorRepr, ranking)

        if (similarityGenerator) {
            val mainSchema = new StructType()
                .add(StructField("user_id", IntegerType, nullable = false))
                .add(StructField("anime_id", IntegerType, nullable = false))
                .add(StructField("rating", IntegerType, nullable = false))

            val rating_complete =  DataLoader.loadCSV(sparkSession, (if (sys.env.contains("localenv")) "" else "gs://silver-vulture-data/") + "data/rating_complete_new.csv", mainSchema)

            vectorRepr.parseDF(rating_complete)
            vectorRepr.save()

            ranking.normalizeRDD()
            ranking.save()



        } else {
            vectorRepr.load()
            vectorRepr.show()
            ranking.load()
            ranking.show()
        }

        if (similarityEvaluation) {
			println("Similarity evaluation")

            predictor.predict(4).show()

        }

        if (sys.env.contains("localenv") && sys.env("localenv") != "nostop") {
            println("Press enter to close")
            System.in.read
        }
        sparkSession.stop()
    }
}
