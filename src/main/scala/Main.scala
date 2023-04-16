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
        val similarityEvaluation = false

        // TODO: Lorenzo non lasciare garbage code
        //val ranking = new Ranking(sparkSession)

        if (similarityGenerator) {
            // Load DataLoader
            val dataLoader = new DataLoader(sparkSession);


            val mainSchema = new StructType()
                .add(StructField("UserID", IntegerType, nullable = false))
                .add(StructField("AnimeID", IntegerType, nullable = false))
                .add(StructField("Rating", IntegerType, nullable = false))

            val rating_complete =
                dataLoader.loadCSV("data/rating_sample_example.csv", mainSchema)

            rating_complete.show();

            val vectorRepr = new VectorRepresentation()
            vectorRepr.parseDF(rating_complete, sparkSession)
            //println("Vector Representation: ")
            // vectorRepr.print()

            //ranking.normalizeRDD(vectorRepr);
            //ranking.saveToFile();
        }

        if (similarityEvaluation) {
            //ranking.loadFromFile()
        }

        sparkSession.stop()
    }
}
