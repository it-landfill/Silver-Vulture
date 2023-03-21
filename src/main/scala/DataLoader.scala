import org.apache.spark.sql.{DataFrame, SparkSession}
// https://spark.apache.org/docs/latest/sql-data-sources-csv.html

object DataLoader {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder()
            .appName("Silver-Vulture")
            .config("spark.master", "local")
            .getOrCreate()

       val df = loadCSV(spark, path="data/anime.csv", headers = true, null)
        df.show()
    }

    def loadCSV(session: SparkSession, path: String, headers: Boolean, filteredColumns: Array[String]): DataFrame = {
        session
            .read
            .option("header", "true")
            .csv(path)
    }
}
