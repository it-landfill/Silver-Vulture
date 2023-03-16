import org.apache.spark.sql.SparkSession
// https://spark.apache.org/docs/latest/sql-data-sources-csv.html
// https://stackoverflow.com/questions/73465937/apache-spark-3-3-0-breaks-on-java-17-with-cannot-access-class-sun-nio-ch-direct

object DataLoader extends App {
	val spark = SparkSession
        .builder()
        .appName("Silver-Vulture")
        .config("spark.master", "local")
        .getOrCreate()

    println("Yay!!")

	val path = "../../../data/anime.csv"

	val df = spark.read.csv(path)
	df.show()
}
