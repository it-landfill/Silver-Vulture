import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

class VectorRepresentation(df: DataFrame) {

    private var rdd: Option[RDD[(Int, Map[Int, Int])]] = None
    private var animeList: Option[RDD[Int]] = None
    private var userList: Option[RDD[(Int, Double)]] = None
    parseDF()
    parseAnimeList()
    parseUserList()

    def print(): Unit = {
        rdd match {
            case Some(rdd) => rdd.foreach(println)
            case None      => println("No data to print")
        }
    }

    def getRdd(): Option[RDD[(Int, Map[Int, Int])]] = rdd

    def getAnimeList(): Option[RDD[Int]] = animeList

    def getUserList(): Option[RDD[(Int, Double)]] = userList

    /** Parse the DataFrame into a RDD
      */
    private def parseDF(): Unit = {
        // Transforms the DataFrame into a RDD
        rdd = Some(
          df.rdd
              .map(row =>
                  (row.getInt(1), (row.getInt(0), row.getInt(2)))
              ) // Convert the df to a list of (anime id, (user id, rating))
              .groupByKey() // Group by anime id
              .mapValues(
                _.toMap
              ) // Convert the list of (user id, rating) for each anime id to a map
              .sortByKey() // Sort the values by anime ID
        )
    }

    /** Parse the RDD into a list of anime IDs
      */
    private def parseAnimeList(): Unit = {

        animeList = Some(
          df.rdd
              .map(row => row.getInt(1)) // Get all the anime IDs in the df
              .distinct() // Remove duplicates
              .sortBy(x => x) // Sort the values by anime ID
        )
    }

    /** Parse the RDD into a list of user IDs
      */
    private def parseUserList(): Unit = {
       userList = Some(
          df.rdd
              .map(row => (row.getInt(0), row.getInt(2))) // Get all the user IDs in the df
              .groupByKey()
              .mapValues(values => values.sum.toDouble / values.size.toDouble)
       )
    }

    def loadFromFile(session:SparkSession): Unit = {
        val context = session.sparkContext
        val path ="data/silver_vulture_data_"
        print()
        val tmp_rdd: Some[RDD[(Int, Map[Int, Int])]] = Some(context.objectFile(path+"_rdd\\part-00000"))
        rdd = tmp_rdd
        val tmp_animelist: Some[RDD[Int]] = Some(context.objectFile(path+"_animelist\\part-00000"))
        animeList = tmp_animelist
        val tmp_userlist: Some[RDD[(Int, Double)]] = Some(context.objectFile(path+"_userlist\\part-00000"))
        userList = tmp_userlist
    }

    def saveToFile(): Unit = {
        val path ="data/silver_vulture_data_"
        rdd.foreach(_.saveAsObjectFile(path+"rdd"))
        animeList.foreach(_.saveAsObjectFile(path+"animelist"))
        userList.foreach(_.saveAsObjectFile(path+"userlist"))
    }
}
