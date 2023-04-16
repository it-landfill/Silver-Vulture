import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.FloatType

// https://data-flair.training/blogs/apache-spark-rdd-vs-dataframe-vs-dataset/

class VectorRepresentation() {

    private var mainDF: Option[DataFrame] = None
    private var userList: Option[DataFrame] = None

    def getMainDF(): DataFrame = mainDF.get

    def getUserList(): DataFrame = userList.get

    /*
    def loadFromFile(session: SparkSession): Unit = {
        val context = session.sparkContext
        val path = "data/silver_vulture_data_"
        print()
        val tmp_rdd: Some[RDD[(Int, Int, Float)]] = Some(
          context.objectFile(path + "_rdd\\part-00000")
        )
        rdd = tmp_rdd
        // val tmp_animelist: Some[RDD[Int]] = Some(context.objectFile(path+"_animelist\\part-00000"))
        // animeList = tmp_animelist
        // val tmp_userlist: Some[collection.Map[Int, Double]] = Some(context.objectFile(path+"_userlist\\part-00000"))
        // userList = tmp_userlist
    }

    def saveToFile(): Unit = {
        val path = "data/silver_vulture_data_"
        rdd.foreach(_.saveAsObjectFile(path + "rdd"))
        // animeList.foreach(_.saveAsObjectFile(path+"animelist"))
        // userList.foreach(_.saveAsObjectFile(path+"userlist"))
    }
     */

    /** Parse the DataFrame into a RDD
      */
    def parseDF(df: DataFrame, sparkSession: SparkSession): Unit = {
        // Transforms the DataFrame into a RDD

        userList = Some(
          df
              .select("UserID", "Rating")
              .groupBy("UserID")
              .avg("Rating")
              .withColumnRenamed("avg(Rating)", "AverageRating")
              .withColumn("AverageRating", col("AverageRating").cast(FloatType))
        )

        import sparkSession.implicits._
        mainDF = Some(df
            .join(
              userList.get,
              df("UserID") === userList.get("UserID"),
              "inner"
            )
            .map(row =>
                (
                  row.getInt(0),
                  row.getInt(1),
                  row.getInt(2),
                  row.getInt(2) - row.getFloat(4)
                )
            )
            .toDF("UserID", "AnimeID", "Rating", "NormalizedRating"))
    }
}
