import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.functions.{col}

object Main {
    def main(args: Array[String]) = {
        // Generate spark session
        val sparkSession = SparkSession
            .builder()
            .appName("Silver-Vulture")
            .config("spark.master", "local[*]")
            .config("spark.deploy.mode", "cluster")
            .config("spark.hadoop.validateOutputSpecs", "false")
            //.config("spark.executor.memory", "6G")
            //.config("spark.driver.memory", "6G")
            .getOrCreate()

        // Set log level (Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN)
        sparkSession.sparkContext.setLogLevel("WARN");

        val similarityGenerator = false
        val similarityEvaluation = true

        val vectorRepr = new VectorRepresentation(sparkSession)
        val ranking = new Ranking(sparkSession, vectorRepr)
        val predictor = new Prediction(sparkSession, vectorRepr, ranking)

        if (similarityGenerator) {
            val mainSchema = new StructType()
                .add(StructField("user_id", IntegerType, nullable = false))
                .add(StructField("anime_id", IntegerType, nullable = false))
                .add(StructField("rating", IntegerType, nullable = false))

            val rating_complete =  DataLoader.loadCSV(sparkSession, (if (sys.env.contains("localenv")) "" else "gs://silver-vulture-data/") + "data/rating_sample_example.csv", mainSchema)

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

            predictor.predict(4)

        }

        if (sys.env.contains("localenv") && sys.env.get("localenv").get != "nostop") {
            println("Press enter to close")
            System.in.read
        }
        sparkSession.stop()
    }
}
