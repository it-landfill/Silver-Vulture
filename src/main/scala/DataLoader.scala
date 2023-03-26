import org.apache.spark.sql.{DataFrame, SparkSession}
// https://spark.apache.org/docs/latest/sql-data-sources-csv.html

class DataLoader(session: SparkSession) {
    def loadCSV(
        path: String,
        headers: Boolean,
        filteredColumns: Array[String]
    ): DataFrame = {
        val df = session.read.option("header", headers).option("inferSchema", true).csv(path)
        if (filteredColumns != null) {
            df.select(filteredColumns.head, filteredColumns.tail: _*)
        } else {
            df
        }
    }
}
