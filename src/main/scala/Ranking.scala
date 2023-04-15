import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.types.{FloatType, IntegerType, StructField, StructType}

import scala.math.{pow, sqrt}
class Ranking(session: SparkSession) {
    private val dfSchema = new StructType()
        .add(StructField("AnimeID1", IntegerType, nullable = false))
        .add(StructField("AnimeID2", IntegerType, nullable = false))
        .add(StructField("Similarity", FloatType, nullable = false))

    private var similarityDF:Option[DataFrame] = None

    def topNItem(idItem: Int, maxN: Int = 5, threshold: Float = 0.5f): DataFrame = {
        println(">> topNItem from Ranking.scala");
        val topN = similarityDF.get
            .filter(anime => (anime.getInt(0) == idItem ||  anime.getInt(1) == idItem) && anime.getFloat(2) >= threshold)
            .sort(desc("Similarity"))
            .limit(maxN)
            .drop("AnimeID1")
        topN
    }

    def normalizeRDD(VectorRepresentation: VectorRepresentation): Unit = {
        println(">> normalizeRDD from Ranking.scala");
        // From mean score for each user from RDD VectorRepresentation.getUserList() RDD[(Int, Double)], update the
        // RDD VectorRepresentation.getRdd() with normalized rating values.
        val userMeanScore =
            VectorRepresentation
                .getUserList()
                .get // TODO: Gestire caso in cui sia None

        val userAnimeRatings =
            VectorRepresentation
                .getRdd()
                .get // TODO: Gestire caso in cui sia None

        // for each user in userAnimeRatings, update the rating value with the normalized value (rating - meanScore specific for that user)
        val ratings = userAnimeRatings.mapValues(animeScores =>
            animeScores.map(evaluation =>
                evaluation._1 -> (evaluation._2 - userMeanScore(
                  evaluation._1
                )) // FIXME: This is a Map, it must not be used in large df (like the user one...)
            )
        )

        val cart = ratings
            .cartesian(ratings)
            .filter(pair => pair._1._1 < pair._2._1) // Handle symmetry

        // Better solution since it does not re evaluate the denominator each time but potentially worse since it uses an external Map

        val denominator = ratings
            .mapValues(animeScores =>
                sqrt(animeScores.values.map(rating => pow(rating, 2)).sum)
            )
            .collectAsMap()

        // println(">> Denominator: " + denominator)

        val numerator: RDD[Row] = cart.map(pair =>
            Row(
              pair._1._1,
              pair._2._1,
              (pair._1._2
                  .map(animeScore1 =>
                      if (pair._2._2.contains(animeScore1._1)) {
                          animeScore1._2 * pair._2._2(animeScore1._1)
                      } else {
                          0
                      }
                  )
                  .sum / (
                denominator(pair._1._1) * denominator(pair._2._1)
              )).toFloat
            )
        )

        similarityDF = Some(session.createDataFrame(numerator, dfSchema))
        println("Generated similarity DF:")
        similarityDF.get.printSchema()
        similarityDF.get.show()
    }

    def loadFromFile(): Unit = {
        val path = "data/silver_vulture_data_df"
        val df = session.read.format("csv").option("header", value = true).schema(dfSchema).load(path)
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
                similarityDF.get.write.mode(SaveMode.Overwrite).format("csv").option("header", value = true).save(path)
            case None => println("No data to save")
        }
    }

}
