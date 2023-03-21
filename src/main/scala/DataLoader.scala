import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable
// https://spark.apache.org/docs/latest/sql-data-sources-csv.html


object DataLoader {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Silver-Vulture")
      .config("spark.master", "local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("FATAL")
    val anime = loadCSV(spark, path = "data/anime.csv", headers = true, filteredColumns = Array("MAL_ID", "Genres", "Studios"))
    println("Anime data loading complete!")
    val user_ratings = loadCSV(spark, path = "data/rating_complete.csv", headers = true, null)
    println("User ratings loading complete!")
    println("Now converting to internal representation...")
    val animes = anime.collectAsList().par
    val result = animes.map(anime => new Anime(anime.get(0), anime.get(1), anime.get(2), user_ratings.filter(user_ratings("anime_id")===anime.get(0)))).par

    println("Done.")
  }

  def loadCSV(session: SparkSession, path: String, headers: Boolean, filteredColumns: Array[String]): DataFrame = {
    val df = session
      .read.option("header", "true")
      .csv(path)
    if (filteredColumns != null) {
      df.select(filteredColumns.head, filteredColumns.tail: _*)
    } else {
      df
    }
  }
}
