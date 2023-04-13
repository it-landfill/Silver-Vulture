import org.apache.spark.sql.{DataFrame, SparkSession}
// https://spark.apache.org/docs/latest/sql-data-sources-csv.html

class DataLoader(session: SparkSession) {
    def loadCSV(
        path: String,
        headers: Boolean,
        filteredColumns: Array[String]
    ): DataFrame = {
        val df = session.read.format("csv").option("header", "true").load(path)
        if (filteredColumns != null) {
            df.select(filteredColumns.head, filteredColumns.tail: _*)
        } else {
            df
        }
    }
}
