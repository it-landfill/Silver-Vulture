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
        val rating_complete = dataLoader.loadCSV("data/rating_complete.csv", true, null);
        rating_complete.show();
    }
}
