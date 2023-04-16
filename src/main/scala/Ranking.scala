import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{abs, col, desc}
import org.apache.spark.sql.types.{
    FloatType,
    IntegerType,
    StructField,
    StructType
}

import scala.math.{pow, sqrt}

// Documentation: http://files.grouplens.org/papers/www10_sarwar.pdf

class Ranking(session: SparkSession) {
    private val dfSchema = new StructType()
        .add(StructField("anime_id_1", IntegerType, nullable = false))
        .add(StructField("anime_id_2", IntegerType, nullable = false))
        .add(StructField("similarity", FloatType, nullable = false))

    private var similarityDF: Option[DataFrame] = None

    private var vectorRepresentation: VectorRepresentation = _

    def topNItem(
        idItem: Int,
        maxN: Int = 5,
        enableThreshold: Boolean = false,
        threshold: Float = 0.5f
    ): DataFrame = {

        val topN = similarityDF.get
            .filter(anime =>
                (anime.getInt(0) == idItem || anime.getInt(1) == idItem)
                    && (!enableThreshold || anime.getFloat(2) >= threshold))
            .sort(desc("similarity"))
            .limit(maxN)
        topN
    }

    /*
    def prediction(user: Int, anime: Int): Float = {
        println(">> prediction from Ranking.scala");
        val averageUserScore = vectorRepresentation
            .getUserList()
            .get(user)

        val topN = topNItem(anime, 2, -10f)
            .rdd
            .map(row => (row.getInt(0), row.getFloat(1)))
            .collectAsMap()

        val numerator = vectorRepresentation
            .getRdd()
            .get
            .filter(animeScore => animeScore._2.contains(user) && topN.contains(animeScore._1))
            .map(animeScore => (animeScore._2(user) - averageUserScore) * topN(animeScore._1))

        val denominator = topN.values.map(elem => elem.abs).sum
        // val denominator = topN.values.sum

        // println(">> Average user score: " + averageUserScore)
        // println(">> Numerator: " + numerator.sum)

        // println(">> Denominator: " + denominator)

        (averageUserScore + (numerator.sum / denominator)).toFloat
    }

     */

    def normalizeRDD(vectorRepresentationExt: VectorRepresentation): Unit = {
        // From mean score for each user from RDD VectorRepresentation.getUserList() RDD[(Int, Double)], update the
        // RDD VectorRepresentation.getRdd() with normalized rating values.
        val userMeanScore =
            vectorRepresentationExt.getUserList

        val userAnimeRatings =
            vectorRepresentationExt.getMainDF

        // TODO: Optimize this
        vectorRepresentation = vectorRepresentationExt

        import session.implicits._
        val denom = vectorRepresentation.getMainDF
            .select("anime_id", "normalized_rating")
            .map(row => (row.getInt(0), pow(row.getFloat(1), 2)))
            .groupBy("_1")
            .sum("_2")
            .map(row => (row.getInt(0), sqrt(row.getDouble(1))))
            .toDF("anime_id", "denominator")
            .withColumn("denominator", col("denominator").cast(FloatType))

        val usrLst = vectorRepresentation.getMainDF
            .select("user_id", "anime_id", "normalized_rating")

        val numer = usrLst
            .as("anime_1")
            .join(
              usrLst.as("anime_2"),
              col("anime_1.user_id") === col("anime_2.user_id"),
              "inner"
            )
            .filter(row => row.getInt(1) < row.getInt(4))
            .map(row =>
                (
                  row.getInt(0),
                  row.getInt(1),
                  row.getInt(4),
                  row.getFloat(2) * row.getFloat(5)
                )
            )
            .select(
              col("_2").as("anime_1_id"),
              col("_3").as("anime_2_id"),
              col("_4").as("norm_rating")
            )
            .groupBy("anime_1_id", "anime_2_id")
            .sum("norm_rating")
            .withColumnRenamed("sum(norm_rating)", "numerator")
            .withColumn("numerator", col("numerator").cast(FloatType))

        val aniMatrix = numer
            .join(denom, col("anime_1_id") === col("anime_id"), "inner")
            .drop("anime_id")
            .join(denom, col("anime_2_id") === col("anime_id"), "inner")
            .drop("anime_id")
            .map(row =>
                (
                  row.getInt(0),
                  row.getInt(1),
                  row.getFloat(2) / (row.getFloat(3) * row.getFloat(4))
                )
            )
            .toDF("anime_1_id", "anime_2_id", "similarity")

        similarityDF = Some(aniMatrix)
        println("Generated similarity DF:")
        similarityDF.get.printSchema()
        similarityDF.get.show()

    }

    def loadFromFile(): Unit = {
        val path = "data/silver_vulture_data_df"
        val df = session.read
            .format("csv")
            .option("header", value = true)
            .schema(dfSchema)
            .load(path)
        println("Loaded similarity DF:")
        df.printSchema()
        df.show()
        similarityDF = Some(df)
    }

    def saveToFile(): Unit = {
        val path = "data/silver_vulture_data_df"
        similarityDF match {
            case Some(_) =>
                similarityDF.get.show()
                similarityDF.get.write
                    .mode(SaveMode.Overwrite)
                    .format("csv")
                    .option("header", value = true)
                    .save(path)
            case None => println("No data to save")
        }
    }

}
