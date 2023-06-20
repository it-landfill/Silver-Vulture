import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object Main {
    def main(args: Array[String]) = {
        // Generate spark session
        val sparkSession = SparkSession
            .builder()
            .appName("Silver-Vulture")
            .config("spark.master", "local[*]")
            //.config("spark.deploy.mode", "cluster")
            .config("spark.hadoop.validateOutputSpecs", "false")
            //.config("spark.executor.memory", "6G")
            //.config("spark.driver.memory", "6G")
            .getOrCreate()

        // Set log level (Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN)
        sparkSession.sparkContext.setLogLevel("WARN");

        val similarityGenerator = true
        val similarityEvaluation = false

        val vectorRepr = new VectorRepresentation(sparkSession)
        val ranking = new Ranking(sparkSession, vectorRepr)

        if (similarityGenerator) {
            val mainSchema = new StructType()
                .add(StructField("user_id", IntegerType, nullable = false))
                .add(StructField("anime_id", IntegerType, nullable = false))
                .add(StructField("rating", IntegerType, nullable = false))

            val rating_complete =  DataLoader.loadCSV(sparkSession, if (sys.env.contains("localenv")) "" else "gs://silver-vulture-data/" + "data/rating_complete_new.csv", mainSchema)

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
            ranking.topNItem(5114, 50, enableThreshold = false).show()
            //println(ranking.prediction(1, 1))
            /*
            val a: Array[Float] = new Array[Float](6)
            for (i <- 1 to 6) {
                for (j <- 0 to 5) {
                    a(j) = ranking.prediction(i, j + 1)
                }
                println(a.mkString("(", "\t", ")"))
            }

             */

        }

        if (sys.env.contains("localenv")) {
            println("Press enter to close")
            System.in.read
        }
        sparkSession.stop()
    }
}
