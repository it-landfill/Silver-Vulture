import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
// https://spark.apache.org/docs/latest/sql-data-sources-csv.html

object DataLoader {
    def loadCSV(
        session: SparkSession,
        path: String,
        schema: StructType,
        headers: Boolean = true,
        filteredColumns: Array[String] = null
    ): DataFrame = {
        val df = session.read.format("csv").option("header", headers).schema(schema).load(path)
        if (filteredColumns != null) {
            df.select(filteredColumns.head, filteredColumns.tail: _*)
        } else {
            df
        }
    }

    def saveCSV(df: Option[DataFrame], path: String): Unit = {
        println("Saving " + path)
        df match {
            case Some(df) =>
                df.write
                    .mode(SaveMode.Overwrite)
                    .format("csv")
                    .option("header", value = true)
                    .save(path)
            case None => println("No data to save for " + path)
        }
    }
}
