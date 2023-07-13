import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{FloatType, StructType, IntegerType, StructField}

// https://data-flair.training/blogs/apache-spark-rdd-vs-dataframe-vs-dataset/

class VectorRepresentation(sparkSession: SparkSession, localenv: Boolean, bucketName: String) {

    private var mainDF: Option[DataFrame] = None
    private var userDF: Option[DataFrame] = None

    /** Returns a DataFrame with the following columns:<br>
      *   - user_id: Int<br>
      *   - anime_id: Int<br>
      *   - rating: Int<br>
      *   - normalized_rating: Float
      */
    def getMainDF: DataFrame = mainDF.get // TODO: Gestire caso in cui sia None

    /** Returns a DataFrame with the following columns:<br>
      *   - user_id: Int<br>
      *   - average_rating: Float
      */
    def getUserList: DataFrame =
        userDF.get // TODO: Gestire caso in cui sia None

    /** Parse the DataFrame into a RDD
      */
    def parseDF(df: DataFrame): Unit = {

        userDF = Some(
          df
              .select("user_id", "rating")
              .groupBy("user_id")
              .avg("rating")
              .withColumnRenamed("avg(rating)", "average_rating")
              .withColumn(
                "average_rating",
                col("average_rating").cast(FloatType)
              )
        )

        import sparkSession.implicits._
        mainDF = Some(
          df
              .join(
                  userDF.get,
                df("user_id") === userDF.get("user_id"),
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
              .toDF("user_id", "anime_id", "rating", "normalized_rating")
        )
    }

        def show(): Unit = {
            println("Main DF")
            mainDF match {
                case Some(df) =>
                    df.printSchema()
                    df.show()
                case None => println("Main DF not defined")
            }

            println("User DF")
            userDF match {
                case Some(df) =>
                    df.printSchema()
                    df.show()
                case None => println("User DF not defined")
            }
        }

    def load(): Unit = {
        val path = (if (localenv) "" else ("gs://"+bucketName+"/")) + "data/silver_vulture_data_"

        val mainSchema = new StructType()
            .add(StructField("user_id", IntegerType, nullable = false))
            .add(StructField("anime_id", IntegerType, nullable = false))
            .add(StructField("rating", IntegerType, nullable = false))
            .add(StructField("normalized_rating", FloatType, nullable = false))

        val userSchema = new StructType()
            .add(StructField("user_id", IntegerType, nullable = false))
            .add(StructField("average_rating", FloatType, nullable = false))

        mainDF = Some(DataLoader.loadCSV(sparkSession, path+"mainDF", mainSchema))
        userDF = Some(DataLoader.loadCSV(sparkSession, path+"userDF", userSchema))
    }
        def save(): Unit = {
            val path = (if (localenv) "" else ("gs://"+bucketName+"/")) + "data/silver_vulture_data_"
            DataLoader.saveCSV(mainDF, path+"mainDF")
            DataLoader.saveCSV(userDF, path+"userDF")
        }
}
