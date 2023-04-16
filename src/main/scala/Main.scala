import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

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

        val similarityGenerator = true
        val similarityEvaluation = true

        // TODO: Lorenzo non lasciare garbage code
        val vectorRepr = new VectorRepresentation()
        val ranking = new Ranking(sparkSession, vectorRepr)

        if (similarityGenerator) {
            // Load DataLoader
            val dataLoader = new DataLoader(sparkSession);


            val mainSchema = new StructType()
                .add(StructField("user_id", IntegerType, nullable = false))
                .add(StructField("anime_id", IntegerType, nullable = false))
                .add(StructField("rating", IntegerType, nullable = false))

            val rating_complete = dataLoader.loadCSV("data/rating_sample_example.csv", mainSchema)

            vectorRepr.parseDF(rating_complete, sparkSession)
            //println("Vector Representation: ")
            // vectorRepr.print()

            ranking.normalizeRDD()
            //ranking.saveToFile();
        }

        if (similarityEvaluation) {
            //ranking.loadFromFile()
            //ranking.topNItem(2, enableThreshold = false).show()
            println(ranking.prediction(1, 1))
        }

        sparkSession.stop()
    }
}
