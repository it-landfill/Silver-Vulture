import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.FloatType

// https://data-flair.training/blogs/apache-spark-rdd-vs-dataframe-vs-dataset/

class VectorRepresentation() {

    private var mainDF: Option[DataFrame] = None
    private var userList: Option[DataFrame] = None

    /**
     * Returns a DataFrame with the following columns:<br>
     * - user_id: Int<br>
     * - anime_id: Int<br>
     * - rating: Int<br>
     * - normalized_rating: Float
     * */
    def getMainDF: DataFrame = mainDF.get // TODO: Gestire caso in cui sia None

    /**
     * Returns a DataFrame with the following columns:<br>
     * - user_id: Int<br>
     * - average_rating: Float
     */
    def getUserList: DataFrame = userList.get // TODO: Gestire caso in cui sia None

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
              .select("user_id", "rating")
              .groupBy("user_id")
              .avg("rating")
              .withColumnRenamed("avg(rating)", "average_rating")
              .withColumn("average_rating", col("average_rating").cast(FloatType))
        )

        import sparkSession.implicits._
        mainDF = Some(df
            .join(
              userList.get,
              df("user_id") === userList.get("user_id"),
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
            .toDF("user_id", "anime_id", "rating", "normalized_rating"))
    }
}
