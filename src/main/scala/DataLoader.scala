import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
// https://spark.apache.org/docs/latest/sql-data-sources-csv.html

class DataLoader(session: SparkSession) {
    def loadCSV(
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
}
